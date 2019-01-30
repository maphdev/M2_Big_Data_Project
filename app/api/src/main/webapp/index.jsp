<%--
  Created by IntelliJ IDEA.
  User: manon
  Date: 26/01/2019
  Time: 23:11
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Title</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.4.0/dist/leaflet.css"
          integrity="sha512-puBpdR0798OZvTTbP4A8Ix/l+A4dHDD0DGqYW6RQ+9jxkRFclaxxQb/SJAWZfWAkuyeQUytO7+7N4QKrDh+drA=="
          crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.4.0/dist/leaflet.js"
            integrity="sha512-QVftwZFqvtRNi0ZyCtsznlKSWOStnDORoefr1enyq5mVL4tmKB3S/EnC3rRJcxCPavG10IcrVGSmPh6Qw5lwrg=="
            crossorigin=""></script>
    <style>
        #mapid {
            height: 800px;
            background-color: rgb(14, 93, 183);
        }
    </style>
</head>
<body>
    <div id="mapid"></div>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>

    <script type="text/javascript">
        var map = L.map('mapid', {minZoom: 0, maxZoom: 11}).setView([0, 0], 0);

        L.TileLayer.Kitten = L.TileLayer.extend({

            getTileUrl: function(coords) {
                console.log(coords.x + " " + coords.y + " " + coords.z);
                return '/Project/webapi/tiles/' + coords.x + '/' + coords.y + '/' + coords.z;
            }
        });

        L.tileLayer.kitten = function() {
            return new L.TileLayer.Kitten();
        }

        L.tileLayer.kitten().addTo(map);
    </script>
</body>
</html>
