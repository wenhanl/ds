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
    # min_ip = 'ec2-52-6-175-32.compute-1.amazonaws.com/'
    # min_ip = 'localhost/server/php/'

    render html:min_ip
  end
end
