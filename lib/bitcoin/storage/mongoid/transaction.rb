class Transaction
  include Mongoid::Document

  field :hash,        type: Binary
  field :version,     type: BigInteger
  field :lock_time,   type: BigInteger
  field :coinbase,    type: Boolean
  field :size,        type: Integer

  validates :hash, unique: true

  has_and_belongs_to_many :blocks, class_name: 'Block'

  has_many :transaction_outs
  has_many :transaction_ins
end
