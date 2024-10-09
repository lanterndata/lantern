require 'sequel'
require 'pry'

DB = Sequel.connect(ENV['DATABASE_URL'])

RSpec.describe 'An example of the RSpec' do
  it 'should include "test" in the array' do
    array = %w[test example demo]
    expect(array).to(include('test'))
  end
end

# require_relative '../main' # Update this path to the actual file name

# describe PG do
#   describe '.count_and_select_all' do
#     it 'returns a hash of class names with counts and other details' do
#       result = PG.count_and_select_all
#       expect(result).to be_a(Hash)
#       expect(result.keys.first).to start_with('PG::Pg')
#       expect(result.values.first).to have_key(:count)
#       expect(result.values.first).to have_key(:all)
#       expect(result.values.first).to have_key(:table_name)
#     end
#   end
# end
#
describe 'all_vs_first' do
  it 'meh' do
    a = DB.fetch('SELECT 1')
    b = DB.fetch('SELECT 1')
    expect(a.all).to eq(b.all)
  end
end

RSpec.describe 'Sequel Eager Loading Tests' do
  before(:all) do
    # Define the schema for artists, albums, genres, and tracks
    DB.run('DROP TABLE IF EXISTS artists CASCADE')
    DB.create_table(:artists) do
      primary_key :id
      String :name
    end

    DB.run('DROP TABLE IF EXISTS albums CASCADE')
    DB.create_table(:albums) do
      primary_key :id
      foreign_key :artist_id, :artists
      # foreign_key :genre_id, :genres
      Integer :genre_id
      Integer :year
      String :name
    end

    # DB.create_table(:genres) do
    #   primary_key :id
    #   String :name
    # end

    # DB.create_table(:tracks) do
    #   primary_key :id
    #   foreign_key :album_id, :albums
    #   Integer :number
    #   String :name
    # end

    # Define the models
    class Artist < Sequel::Model
      one_to_many :albums
    end

    class Album < Sequel::Model
      many_to_one :artist
      many_to_one :genre
      one_to_many :tracks
    end

    class Genre < Sequel::Model
      one_to_many :albums
    end

    class Track < Sequel::Model
      many_to_one :album
    end

    # Insert dummy data
    genre1 = Genre.create(name: 'Rock')
    genre2 = Genre.create(name: 'Jazz')

    artist1 = Artist.create(name: 'The Beatles')
    artist2 = Artist.create(name: 'Miles Davis')

    album1 = Album.create(name: 'Abbey Road', artist_id: artist1.id, genre_id: genre1.id, year: 1969)
    album2 = Album.create(name: 'Kind of Blue', artist_id: artist2.id, genre_id: genre2.id, year: 1959)

    Track.create(name: 'Come Together', album_id: album1.id, number: 1)
    Track.create(name: 'Something', album_id: album1.id, number: 2)
    Track.create(name: 'So What', album_id: album2.id, number: 1)
    Track.create(name: 'Freddie Freeloader', album_id: album2.id, number: 2)
  end

  describe 'Eager and Eager_Graph Loading' do
    it 'checks that Model.first works as expected with eager' do
      expect(Album.first.to_hash).to have_key(:artist_id)
      expect(Album.first.to_hash).not_to have_key(:artist_name)

      eager = Album.eager_graph(:artist)

      expect(eager.first.to_hash).to have_key(:artist_id)
      expect(eager.first.to_hash).to have_key(:artist_name)
    end

    it 'fails since Model.all returns columns different from model.first' do
      # the test above, after s/first/all.first/
      expect(Album.all.first.to_hash).to have_key(:artist_id)
      expect(Album.all.first.to_hash).not_to have_key(:artist_name)

      eager = Album.eager_graph(:artist)

      expect(eager.all.first.to_hash).to have_key(:artist_id)
      expect(eager.all.first.to_hash).to have_key(:artist_name)
    end

    it 'IN PROGRESSS..more experiments on the subject' do
      # the test above, after s/first/all.first/
      expect(Album.all.first.to_hash).to have_key(:artist_id)
      expect(Album.all.first.to_hash).not_to have_key(:artist_name)

      eager = Album.eager_graph(:artist).select_all
      # binding.pry

      expect(eager.all.first.to_hash).to have_key(:artist_id)
      expect(eager.all.first.to_hash).to have_key(:artist_name)
      # expect(eager.all.first.to_hash).to have_key(:artist_name)
    end
  end
end
