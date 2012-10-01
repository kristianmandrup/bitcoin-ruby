class TransactionOut
  include Mongoid::Document

  field :pk_script, type: Binary
  field :value,     type: BigInteger
  field :type,      type: Integer

  belongs_to :transaction
  has_and_belongs_to_many :addresses 

  # index :pk_script 
end
