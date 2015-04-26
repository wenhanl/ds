/**
 * Created by wenhanl on 15-4-23.
 */


var nodeSize = gon.nodesize;



$(function () {
    keylist = new Array();
    valuelist = new Array();
    for (key in nodeSize) {
        keylist.push(String(key));
        valuelist.push(Number(nodeSize[key]));
    }

    sizelist = new Array()
    for (key in nodeSize) {
        sizelist.push([String(key), Number(nodeSize[key])]);
    }

    drawGraph('.graph', sizelist);

    var options = {
        valueNames: [ 'name', 'born' ]
    };

    var userList = new List('users', options);

});


function drawGraph(selector, sizelist) {
    $(selector).highcharts({
        chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false
        },
        title: {
            text: 'Storage Size Distribution'
        },
        tooltip: {
            pointFormat: '{series.name}: <b>{point.y} KB</b>'
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                    style: {
                        color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'
                    }
                }
            }
        },
        series: [{
            type: 'pie',
            name: 'Storage',
            data: sizelist
        }]
    });
}
