$(function() {
    var Night = (function() {
        //
        // Constructor.
        //
        function Night(n) {
            this.n = n;
            this.exposures = [];
            this.div = $("<div/>", {"id": ""+this.n, "class": "row"});
            this.buttons = $("<div/>", {"class": "col-4"});
            this.table = $("<div/>", {"class": "col-8"});
        }
        var N = Night, No = Night.prototype;
        //
        // Add exposure, being careful not to add twice.
        //
        No.addExposure = function(E) {
            var i = this.hasExposure(E.e);
            if (i == -1) {
                this.exposures.push(E);
            } else {
                this.exposures[i].addStage(E);
            }
        };
        //
        // If exposure already exists, return the index.
        //
        No.hasExposure = function(e) {
            for (var k = 0; k < this.exposures.length; k++) {
                if (this.exposures[k].e == e) return k;
            }
            return -1;
        };
        //
        // Night is successful if all exposures are successful.
        //
        No.success = function() {
            for (var k = 0; k < this.exposures.length; k++) {
                if (!this.exposures[k].status) return false;
            }
            return true;
        };
        //
        // Complete construction of tables, etc.
        //
        No.finish = function() {
            this.buttons.append(this.button_html());
            this.table.append(this.table_rows());
            this.div.append(this.buttons);
            this.div.append(this.table);
            this.div.appendTo("#content");
        };
        //
        // Paragraph containing show/hide buttons.
        //
        No.button_html = function() {
            var color = this.success() ? "btn-success" : "btn-danger";
            var p =  "<p id=\"p" + this.n + "\">Night " + this.n + "&nbsp;" +
                     "<button type=\"button\" class=\"btn " + color +
                     " btn-sm\" id=\"show" + this.n +
                     "\" style=\"display:inline;\" onclick=\"$('#t" + this.n +
                     "').css('display', 'block');$('#hide" + this.n +
                     "').css('display', 'inline');$('#show" + this.n +
                     "').css('display', 'none');\">Show</button>" +
                     "<button type=\"button\" class=\"btn " + color +
                     " btn-sm\" id=\"hide" + this.n +
                     "\" style=\"display:none;\" onclick=\"$('#t" + this.n +
                     "').css('display', 'none');$('#show" + this.n +
                     "').css('display', 'inline');$('#hide" + this.n +
                     "').css('display', 'none');\">Hide</button></p>";
            return p;
        };
        //
        // Table of individual exposures.
        //
        No.table_rows = function() {
            var r = "<table id=\"t" + this.n + "\" style=\"display:none;\">" +
                    this.exposures[0].header() + "<tbody>";
            for (var k = 0; k < this.exposures.length; k++) {
                r += this.exposures[k].row();
            }
            r += "</tbody></table>";
            return r;
        };
        return Night;
    })();
    var Exposure = (function() {
        //
        // Constructor.
        //
        function Exposure(r) {
            this.n = r[0];
            this.e = r[1];
            this.stage = {};
            for (var k = 0; k < Exposure.stages.length; k++) {
                this.stage[Exposure.stages[k]] = {"success": false, "stamp": 0};
            }
            if (Exposure.stages.indexOf(r[2]) == -1) {
                alert("Invalid stage '" + r[2] + "' in " + r[0] + "/" + r[1] + "!");
            } else {
                this.stage[r[2]] = {"success": r[3], "stamp": r[5]};
            }
            // this.c = this.status ? "bg-success" : "bg-danger";
            this.l = r[4].length > 0 ? " Last " + r[4] + " exposure." : "";
        }
        var E = Exposure, Eo = Exposure.prototype;
        E.padding = 8;
        E.stages = ["rsync", "checksum", "pipeline", "backup"];
        //
        // Pad integers out to 8 characters.
        //
        Eo.pad = function() {
            var pe = ("" + this.e).split("");
            while (pe.length < Exposure.padding) pe.unshift("0");
            return pe.join("");
        };
        //
        // Header for status table.
        //
        Eo.header = function() {
            var h = "<thead><tr>";
            for (var k = 0; k < Exposure.stages.length; k++) {
                h += "<th class=\"text-uppercase\">" + Exposure.stages + "</th>";
            }
            h += "</tr></thead>";
            return h;
        };
        //
        // Row in the status table.
        //
        Eo.row = function() {
            var r = "<tr id=\"e" + this.toString() +"\">" +
                    "<td>" + this.pad() + "</td>";
            for (var k = 0; k < Exposure.stages.length; k++) {
                var c = "bg-warning";
                var stamp = "INCOMPLETE";
                if (this.stage[Exposure.stages[k]].stamp != 0) {
                    c = this.stage[Exposure.stages[k]].success ? "bg-success" : "bg-danger";
                    var d = new Date(this.stage[Exposure.stages[k]].stamp);
                    stamp = d.toISOString();
                }
                r +=  "<td class=\"" + c + "\">" + stamp + "</td>";
            }
            r += "<td>" + this.l + "</td></tr>";
            return r;
        };
        //
        // Add additional stage data to an existing exposure.
        //
        Eo.addStage = function(stage) {
            if (stage.n == this.n && stage.e == this.e) {
                for (var k = 0; k < Exposure.stages.length; k++) {
                    var s = Exposure.stages[k]
                    //
                    // Does stage have a defined timestamp?
                    //
                    if (stage.stage[s].stamp != 0) {
                        //
                        // Is it more recent?  This will also be true if this
                        // has an undefined timestamp.
                        //
                        if (stage.stage[s].stamp > this.stage[s].stamp) {
                            this.stage[s].success = stage.stage[s].success;
                            this.stage[s].stamp = stage.stage[s].stamp;
                        }
                    }
                }
            } else {
                alert("Can't add " + stage.toString() + " to " + this.toString() + "!");
            }
        };
        //
        // Format night/exposure.
        //
        Eo.toString = function() {
            return "" + this.n + "/" + this.pad();
        };
        return Exposure;
    })();
    var Status = {
        //
        // Raw data read from JSON file.
        //
        raw: [],
        //
        // List of Night objects.
        //
        nights: [],
        //
        // If a night already exists in the nights array, return the index.
        //
        hasNight: function(n) {
            for (var k = 0; k < this.nights.length; k++) {
                if (this.nights[k].n == n) return k;
            }
            return -1;
        }
    };
    //
    // Main display function.
    //
    display = function() {
        // if (typeof this.raw === "undefined") alert("this.raw undefined!");
        // if (typeof this.nights === "undefined") alert("this.nights undefined!");
        // if (typeof this.hasNight === "undefined") alert("this.hasNight undefined!");
        // if (typeof this.display === "undefined") alert("this.display undefined!");
        $("#content").empty();
        var night;
        for (var k = 0; k < Status.raw.length; k++) {
            var n = Status.raw[k][0];
            if (Status.hasNight(n) == -1) {
                //
                // Finish previous night
                //
                if (Status.nights.length > 0) night.finish();
                //
                // Start a new night
                //
                night = new Night(n);
                Status.nights.push(night);
            }
            //
            // Add exposure to existing night.
            //
            var e = new Exposure(this.raw[k]);
            night.addExposure(e);
        }
        //
        // Finish the final night
        //
        night.finish();
    };
    $.getJSON("desi_transfer_status.json", {}, function(data) { Status.raw = data; }).always(display);
    return true;
});
