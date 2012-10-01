class Address
  field :hash160, type: String

  has_and_belongs_to_many :transaction_outs
end