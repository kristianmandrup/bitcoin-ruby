class Block
  include Mongoid::Document

  field :hash,        type: Binary
  field :depth,       type: Integer
  field :version,     type: BigInteger
  field :prev_hash,   type: Binary
  field :mrkl_root,   type: Binary
  field :time,        type: BigInteger
  field :bits,        type: BigInteger
  field :nonce,       type: BigInteger
  field :size,        type: Integer
  field :chain,       type: Integer

  validates :hash, unique: true

  validates_presence_of :hash, :depth, :version,  :prev_hash, :mrkl_root
  validates_presence_of :time, :bits,  :nonce,    :size,      :chain

  has_and_belongs_to_many :transactions
  # index :depth, :hash, :prev_hash
end
