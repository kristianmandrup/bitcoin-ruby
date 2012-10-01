module Bitcoin::Storage::Backends

  # Storage backend using Sequel to connect to arbitrary SQL databases.
  # Inherits from StoreBase and implements its interface.
  class MongoidStore < StoreBase

    # possible script types
    SCRIPT_TYPES = [:unknown, :pubkey, :hash160, :multisig, :p2sh]

    # sequel database connection
    attr_accessor :db

    DEFAULT_CONFIG = {mode: :full}

    # create sequel store with given +config+
    def initialize config, *args
      @config = DEFAULT_CONFIG.merge(config)
      connect
      super config, *args
    end

    # connect to database
    def connect
      @db = Mongoid.connect(@config[:db]) # TODO
    end

    # reset database; delete all data
    def reset
      [:blk, :blk_tx, :tx, :txin, :txout].each {|table| @db[table].delete}
      @head = nil
    end

    # persist given block +blk+ to storage.
    def persist_block blk, chain, depth
      @db.transaction do
        attrs = {
          :hash => blk.hash.htb.to_sequel_blob,
          :depth => depth,
          :chain => chain,
          :version => blk.ver,
          :prev_hash => blk.prev_block.reverse.to_sequel_blob,
          :mrkl_root => blk.mrkl_root.reverse.to_sequel_blob,
          :time => blk.time,
          :bits => blk.bits,
          :nonce => blk.nonce,
          :blk_size => blk.to_payload.bytesize,
        }
        existing = @db[:blk].filter(:hash => blk.hash.htb.to_sequel_blob)
        if existing.any?
          existing.update attrs
          block_id = existing.first[:id]
        else
          block_id = @db[:blk].insert(attrs)

          blk.tx.each_with_index do |tx, idx|
            tx_id = store_tx(tx)
            raise "Error saving tx #{tx.hash} in block #{blk.hash}"  unless tx_id
            @db[:blk_tx].insert({
                :blk_id => block_id,
                :tx_id => tx_id,
                :idx => idx,
              })
          end
        end
        @head = wrap_block(attrs.merge(id: block_id))  if chain == MAIN
        @db[:blk].where(:prev_hash => blk.hash.htb.to_sequel_blob, :chain => ORPHAN).each do |b|
          log.debug { "re-org orphan #{b[:hash].hth}" }
          begin
            store_block(get_block(b[:hash].hth))
          rescue SystemStackError
            EM.defer { store_block(get_block(b[:hash].hth)) }  if EM.reactor_running?
          end
        end
        log.info { "block #{blk.hash} (#{depth}, #{['main', 'side', 'orphan'][chain]})" }
        return depth, chain
      end
    end

    # update +attrs+ for block with given +hash+.
    def update_blocks updates
      updates.each do |blocks, attrs|
        Block.where(:hash => blocks.map{|h| h.htb}).update(attrs)
      end
    end

    # store transaction +tx+
    def store_tx(tx, validate = false)
      @log.debug { "Storing tx #{tx.hash} (#{tx.to_payload.bytesize} bytes)" }
      tx.validator(self).validate(raise_errors: true)  if validate
        transaction = Transaction.where(:hash => tx.hash.htb)
        return transaction[:id]  if transaction
        tx_id = Transaction.create({
            :hash => tx.hash.htb.to_sequel_blob,
            :version => tx.ver,
            :lock_time => tx.lock_time,
            :coinbase => tx.in.size==1 && tx.in[0].coinbase?,
            :tx_size => tx.payload.bytesize,
          })
        tx.in.each_with_index {|i, idx| store_txin(tx_id, i, idx)}
        tx.out.each_with_index {|o, idx| store_txout(tx_id, o, idx)}
        tx_id
      end
    end

    # store input +txin+
    def store_txin(tx_id, txin, idx)
      TransactionIn.create({
          :tx_id => tx_id,
          :tx_idx => idx,
          :script_sig => txin.script_sig,
          :prev_out => txin.prev_out,
          :prev_out_index => txin.prev_out_index,
          :sequence => txin.sequence.unpack("I")[0],
        })

      if @config[:mode] == :pruned
        # delete previous transaction if all its outputs are spent now
        prev_tx = Transaction.where(:hash => txin.prev_out.reverse)
        return  unless prev_tx
        if TransactionOut.where(:tx_id => prev_tx[:id]).map.with_index{|o, i|
            TransactionIn.where(:prev_out => prev_tx[:hash].reverse, :prev_out_index => i).any? }.all?
          delete_tx(prev_tx[:hash].hth)
        end
      end
    end

    # store output +txout+
    def store_txout(tx_id, txout, idx)
      script = Bitcoin::Script.new(txout.pk_script)
      txout_id = TransactionOut.create({
          :tx_id => tx_id,
          :tx_idx => idx,
          :pk_script => txout.pk_script,
          :value => txout.value,
          :type => SCRIPT_TYPES.index(script.type)
        })
      if script.is_hash160? || script.is_pubkey?
        store_addr(txout_id, script.get_hash160)
      elsif script.is_multisig?
        script.get_multisig_pubkeys.map do |pubkey|
          store_addr(txout_id, Bitcoin.hash160(pubkey.unpack("H*")[0]))
        end
      end
      txout_id
    end

    # store address +hash160+
    def store_addr(txout_id, hash160)
      addr = Address.where(:hash160 => hash160)
      addr_id = addr[:id]  if addr
      addr_id ||= @Address.create({:hash160 => hash160})
    end

    def delete_tx(hash)
      log.debug { "Deleting tx #{hash} since all its outputs are spent" }
      @db.transaction do
        tx = get_tx(hash)
        tx.in.each {|i| TransactionIn.where(:id => i.id).delete }
        tx.out.each {|o| TransactionOut.where(:id => o.id).delete }
        Transaction.where(:id => tx.id).delete
      end
    end

    # check if block +blk_hash+ exists
    def has_block(blk_hash)
      !!Block.where(:hash => blk_hash.htb).get(1)
    end

    # check if transaction +tx_hash+ exists
    def has_tx(tx_hash)
      !!Transaction.where(:hash => tx_hash.htb).get(1)
    end

    # get head block (highest block from the MAIN chain)
    def get_head
      @head ||= wrap_block(@db[:blk].filter(:chain => MAIN).order(:depth).last)
    end

    # get depth of MAIN chain
    def get_depth
      return -1  unless get_head
      get_head.depth
    end

    # get block for given +blk_hash+
    def get_block(blk_hash)
      wrap_block Block.where(:hash => blk_hash.htb)
    end

    # get block by given +depth+
    def get_block_by_depth(depth)
      wrap_block Block.where(:depth => depth, :chain => MAIN)
    end

    # get block by given +prev_hash+
    def get_block_by_prev_hash(prev_hash)
      wrap_block Block.where(:prev_hash => prev_hash.htb, :chain => MAIN)
    end

    # get block by given +tx_hash+
    def get_block_by_tx(tx_hash)
      tx = Transaction.where(:hash => tx_hash.htb)
      return nil  unless tx
      parent = @db[:blk_tx][:tx_id => tx[:id]]
      return nil  unless parent
      wrap_block(@db[:blk][:id => parent[:blk_id]])
    end

    # get block by given +id+
    def get_block_by_id(block_id)
      wrap_block(@db[:blk][:id => block_id])
    end

    # get transaction for given +tx_hash+
    def get_tx(tx_hash)
      wrap_tx Transaction.where(:hash => tx_hash.htb)
    end

    # get transaction by given +tx_id+
    def get_tx_by_id(tx_id)
      wrap_tx Transaction.where(:id => tx_id)
    end

    # get corresponding Models::TxIn for the txout in transaction
    # +tx_hash+ with index +txout_idx+
    def get_txin_for_txout(tx_hash, txout_idx)
      tx_hash = tx_hash.htb_reverse
      wrap_txin TransactionIn.where(:prev_out => tx_hash, :prev_out_index => txout_idx)
    end

    # get corresponding Models::TxOut for +txin+
    def get_txout_for_txin(txin)
      tx = Transaction.where(:hash => txin.prev_out.reverse)
      return nil  unless tx
      wrap_txout TransactionOut.where(:tx_idx => txin.prev_out_index, :tx_id => tx[:id])
    end

    # get all Models::TxOut matching given +script+
    def get_txouts_for_pk_script(script)
      txouts = TransactionOut.where(:pk_script => script.to_sequel_blob).order(:id)
      txouts.map{|txout| wrap_txout(txout)}
    end

    # get all Models::TxOut matching given +hash160+
    def get_txouts_for_hash160(hash160, unconfirmed = false)
      addr = Address.where(:hash160 => hash160)
      return []  unless addr
      txouts = @db[:addr_txout].where(:addr_id => addr[:id])
        .map{|t| @db[:txout][:id => t[:txout_id]] }
        .map{|o| wrap_txout(o) }
      unless unconfirmed
        txouts.select!{|o| o.get_tx.get_block.chain == MAIN rescue false }
      end
      txouts
    end

    # get all unconfirmed Models::TxOut
    def get_unconfirmed_tx
      @db[:unconfirmed].map{|t| wrap_tx(t)}
    end

    # wrap given +block+ into Models::Block
    def wrap_block(block)
      return nil  unless block

      data = {:id => block[:id], :depth => block[:depth], :chain => block[:chain]}
      blk = Bitcoin::Storage::Models::Block.new(self, data)

      blk.ver = block[:version]
      blk.prev_block = block[:prev_hash].reverse
      blk.mrkl_root = block[:mrkl_root].reverse
      blk.time = block[:time].to_i
      blk.bits = block[:bits]
      blk.nonce = block[:nonce]

      db[:blk_tx].filter(blk_id: block[:id]).join(:tx, id: :tx_id)
        .order(:idx).each {|tx| blk.tx << wrap_tx(tx, block[:id]) }

      blk.recalc_block_hash
      blk
    end

    # wrap given +transaction+ into Models::Transaction
    def wrap_tx(transaction, block_id = nil)
      return nil  unless transaction

      block_id ||= @db[:blk_tx].join(:blk, id: :blk_id)
        .where(tx_id: transaction[:id], chain: 0).first[:blk_id] rescue nil

      data = {id: transaction[:id], blk_id: block_id}
      tx = Bitcoin::Storage::Models::Tx.new(self, data)

      inputs = db[:txin].filter(:tx_id => transaction[:id]).order(:tx_idx)
      inputs.each { |i| tx.add_in(wrap_txin(i)) }

      outputs = db[:txout].filter(:tx_id => transaction[:id]).order(:tx_idx)
      outputs.each { |o| tx.add_out(wrap_txout(o)) }
      tx.ver = transaction[:version]
      tx.lock_time = transaction[:lock_time]
      tx.hash = tx.hash_from_payload(tx.to_payload)
      tx
    end

    # wrap given +input+ into Models::TxIn
    def wrap_txin(input)
      return nil  unless input
      data = {:id => input[:id], :tx_id => input[:tx_id], :tx_idx => input[:tx_idx]}
      txin = Bitcoin::Storage::Models::TxIn.new(self, data)
      txin.prev_out = input[:prev_out]
      txin.prev_out_index = input[:prev_out_index]
      txin.script_sig_length = input[:script_sig].bytesize
      txin.script_sig = input[:script_sig]
      txin.sequence = [input[:sequence]].pack("I")
      txin
    end

    # wrap given +output+ into Models::TxOut
    def wrap_txout(output)
      return nil  unless output
      data = {:id => output[:id], :tx_id => output[:tx_id], :tx_idx => output[:tx_idx],
        :hash160 => output[:hash160], :type => SCRIPT_TYPES[output[:type]]}
      txout = Bitcoin::Storage::Models::TxOut.new(self, data)
      txout.value = output[:value]
      txout.pk_script = output[:pk_script]
      txout
    end

  end

end
