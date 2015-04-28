// This is a manifest file that'll be compiled into application.js, which will include all the files
// listed below.
//
// Any JavaScript/Coffee file within this directory, lib/assets/javascripts, vendor/assets/javascripts,
// or vendor/assets/javascripts of plugins, if any, can be referenced here using a relative path.
//
// It's not advisable to add code directly here, but if you do, it'll appear at the bottom of the
// compiled file.
//
// Read Sprockets README (https://github.com/sstephenson/sprockets#sprockets-directives) for details
// about supported directives.
//
//= require jquery
//= require jquery_ujs
//= require jquery-ui
//= require twitter/bootstrap

//= require jquery-fileupload/basic
//= require masonry/jquery.masonry
//= require video
//= require_tree .


$(function () {
    'use strict';

    $('.list').masonry({
        itemSelector: '.list li'
    });
    $("#dialog").dialog({
        autoOpen: false,
        width: 500
    });
    $("#upload").click(function(){
        $("#dialog").dialog('open');
    });

    if (document.URL.indexOf("view") > -1) {
        $("#gallery").removeClass();
        $("#monitor").removeClass();
        $("#gallery").addClass("active");
    }

    if (document.URL.indexOf("monitor") > -1) {
        $("#gallery").removeClass();
        $("#monitor").removeClass();
        $("#monitor").addClass("active");
    }

    $('#fileupload').click(function() {
        $('#progress .progress-bar').css(
            'width',
                0 + '%'
        );

        var url = ""
        $.ajax({
            url: "/uploader/upload",
            dataType: "html",
            async: false,
            success: function(msg) {
                url = "//" + msg

                $('#fileupload').fileupload({
                    url: url,
                    dataType: 'json',
                    done: function (e, data) {
                        $.each(data.result.files, function (index, file) {
                            $('<p/>').text(file.name).appendTo('#files');
                            $('<p/>').text("Success! Uploaded to " + url).appendTo('#files');
                        });


                    },
                    progressall: function (e, data) {
                        var progress = parseInt(data.loaded / data.total * 100, 10);
                        $('#progress .progress-bar').css(
                            'width',
                                progress + '%'
                        );
                    }
                });
            },
            error: function(result) {
                $('<p/>').text(result).appendTo('#files');
            }
        });
    });
    // Change this to the location of your server-side upload handler:



});