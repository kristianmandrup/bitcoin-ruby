class TransactionIn
  include Mongoid::Document

  field :script_sig,      type: Binary
  field :prev_out,        type: Binary
  field :prev_out_index,  type: BigInteger
  field :sequence,        type: BigInteger

  belongs_to :transaction

  # index :prev_out
end
