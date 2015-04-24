class CreateFiles < ActiveRecord::Migration
  def change
    create_table :files do |t|
      t.string :name
      t.text :nodes
      t.integer :size
    end
  end
end
