class UploaderController < ApplicationController
  def upload
    @nodes = Nodes.all

    min_size = 100000000
    min_ip = ''
    @nodes.each do |node|
      if node.total_size < min_size
        min_size = node.total_size
        min_ip = node.ip
      end
    end

    # debug line
    # min_ip = '52.4.122.58'
    # min_ip = 'localhost/server/php/'

    render html:min_ip
  end
end
