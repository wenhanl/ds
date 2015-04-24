class MediaController < ApplicationController
  def monitor
    @nodes = Nodes.all
    @files = Files.all

    @node_file_list = Array.new
    @nodes.each do |node|
      node.files
    end

    @node_to_file = Hash.new
    @nodes.each { |node| @node_to_file[node.ip] = node.files }

    @file_to_node = Hash.new
    @files.each { |file| @file_to_node[file.name] = file.nodes }

    @nodesize = Hash.new
    @nodes.each { |node| @nodesize[node.ip] = node.total_size }

    gon.node2file = @node_to_file
    gon.file2node = @file_to_node
    gon.nodesize = @nodesize
  end

  def view

  end
end
