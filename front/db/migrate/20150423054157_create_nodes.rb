class CreateNodes < ActiveRecord::Migration
  def change
    create_table :nodes do |t|
      t.string :ip
      t.text :files
      t.integer :total_size
    end
  end
end
