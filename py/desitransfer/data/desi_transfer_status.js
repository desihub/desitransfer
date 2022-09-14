$(function() {
    var Night = (function() {
        //
        // Constructor.
        //
        function Night(n) {
            this.n = n;
            this.exposures = [];
            this.div = $("<div/>", {"id": ""+this.n, "class": "row"});
            this.buttons = $("<div/>", {"class": "col-md-4"});
            this.table = $("<div/>", {"class": "col-md-8"});
        }
        var N = Night, Np = Night.prototype;
        //
        // Add exposure, being careful not to add twice.
        //
        Np.addExposure = function(E) {
            var i = this.hasExposure(E.e);
            if (i == -1) {
                this.exposures.push(E);
            } else {
                alert("Trying to add duplicate exposure " + E.e + " to Night " + this.n + "!");
            }
        };
        //
        // If exposure already exists, return the index.
        //
        Np.hasExposure = function(e) {
            for (var k = 0; k < this.exposures.length; k++) {
                if (this.exposures[k].e == e) return k;
            }
            return -1;
        };
        //
        // Night is successful if all exposures are successful.
        //
        Np.success = function() {
            var s = Exposure.stages;
            var r = "btn-success";
            for (var k = 0; k < this.exposures.length; k++) {
                for (var l = 0; l < s.length; l++) {
                    if (!this.exposures[k].stage[s[l]].success) {
                        //
                        // It's not successful, but is it complete?
                        //
                        if (this.exposures[k].stage[s[l]].stamp > 0)
                            return "btn-danger";
                        r = "btn-warning";
                    }
                }
            }
            return r;
        };
        //
        // Complete construction of tables, etc.
        //
        Np.finish = function() {
            this.buttons.append(this.button_html());
            this.table.append(this.table_rows());
            this.div.append(this.buttons);
            this.div.append(this.table);
            this.div.appendTo("#content");
        };
        //
        // Paragraph containing show/hide buttons.
        //
        Np.button_html = function() {
            var color = this.success();
            return "<p id=\"p" + this.n + "\"><strong>Night " + this.n + "</strong>&nbsp;" +
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
        };
        //
        // Table of individual exposures.
        //
        Np.table_rows = function() {
            var r = $("<table/>", {"id": "t" + this.n, "class": "table table-borderless table-sm", "style": "display:none;"});
            r.append(this.exposures[0].header());
            var body = $("<tbody/>");
            for (var k = 0; k < this.exposures.length; k++) {
                body.append(this.exposures[k].row());
            }
            r.append(body);
            return r;
        };
        return Night;
    })();
    var Exposure = (function() {
        //
        // Constructor.
        //
        function Exposure(n, e, r) {
            this.n = n;
            this.e = e;
            this.stage = {};
            for (var i = 0; i < r.length; i++) {
                var s = Exposure.stages[r[i][0]];
                if (this.stage.hasOwnProperty(s)) {
                    if (r[i][2] > this.stage[s].stamp) {
                        this.stage[s].success = r[i][1] == 1;
                        this.stage[s].stamp = r[i][2];
                    }
                } else {
                    this.stage[s] = {"success": r[i][1] == 1, "stamp": r[i][2]};
                }
            }
            for (var j = 0; j < Exposure.stages.length; j++) {
                var s = Exposure.stages[j];
                if (!this.stage.hasOwnProperty(s)) {
                    this.stage[s] = {"success": false, "stamp": 0};
                }
            }
        }
        var E = Exposure, Ep = Exposure.prototype;
        E.padding = 8;
        E.rawBaseURL = "https://data.desi.lbl.gov/desi/spectro/data/";
        E.stages = ["rsync", "checksum", "backup"];
        //
        // Pad integers out to 8 characters.
        //
        Ep.pad = function() {
            var pe = ("" + this.e).split("");
            while (pe.length < Exposure.padding) pe.unshift("0");
            return pe.join("");
        };
        //
        // URL for actual raw data.
        //
        Ep.rawURL = function() {
            return Exposure.rawBaseURL + this.n + "/" + this.pad() + "/";
        };
        //
        // Header for status table.
        //
        Ep.header = function() {
            var h = $("<thead/>");
            var r = $("<tr/>");
            r.append($("<th/>", {"class": "text-uppercase"}).html("exposure"));
            for (var k = 0; k < Exposure.stages.length; k++) {
                r.append($("<th/>", {"class": "text-uppercase"}).html(Exposure.stages[k]));
            }
            h.append(r);
            return h;
        };
        //
        // Row in the status table.
        //
        Ep.row = function() {
            var r = $("<tr/>", {"id": "e" + this.toString()});
            var link = $("<a/>", {"href": this.rawURL()}).html(this.pad());
            r.append($("<td/>").html(link));
            for (var k = 0; k < Exposure.stages.length; k++) {
                var c = "table-warning";
                var stamp = "INCOMPLETE";
                if (this.stage[Exposure.stages[k]].stamp != 0) {
                    c = this.stage[Exposure.stages[k]].success ? "table-success" : "table-danger";
                    var d = new Date(this.stage[Exposure.stages[k]].stamp);
                    stamp = d.toISOString();
                }
                r.append($("<td/>", {"class": c}).html(stamp));
            }
            return r;
        };
        //
        // Format night/exposure.
        //
        Ep.toString = function() {
            return "" + this.n + "/" + this.pad();
        };
        return Exposure;
    })();
    var Status = {
        //
        // Display status.
        //
        displayAll: false,
        //
        // Display year.
        //
        displayYear: "2022",
        //
        // Raw data read from JSON file.
        //
        // raw: [],
        raw: {"2022": {}},
        //
        // List of Night objects.
        //
        nights: {"2022": []},
        //
        // If a night already exists in the nights array, return the index.
        //
        hasNight: function(n) {
            for (var k = 0; k < this.nights.length; k++) {
                if (this.nights[k].n == n) return k;
            }
            return -1;
        },
        //
        // Per-year data file.
        //
        dataFile: function() {
            return "desi_transfer_status_" + this.displayYear + ".json";
        }
    };
    //
    // Main display function.
    //
    var display = function() {
        var default_display = 10;
        $("#content").empty();
        var h2 = "Showing " +
                 (Status.displayAll ? "all" : "most recent 10") +
                 " Nights from " +
                 Status.displayYear;
        $("#displayTitle").html(h2);
        var nights = Status.nights[Status.displayYear];
        nights = [];
        var night;
        var raw = Status.raw[Status.displayYear];
        var all_nights = Object.keys(raw).sort().reverse();
        // alert(all_nights.join(","));
        var n_display = Status.displayAll ? all_nights.length : default_display;
        if (all_nights.length < default_display) n_display = all_nights.length;
        for (var k = 0; k < n_display; k++) {
            var n = all_nights[k];
            //
            // Start a new night
            //
            night = new Night(n);
            nights.push(night);
            var exposures = Object.keys(raw[n]).sort().reverse();
            for (var l = 0; l < exposures.length; l++) {
                //
                // Add exposure to existing night.
                //
                var e = new Exposure(n, exposures[l], raw[n][exposures[l]]);
                night.addExposure(e);
            }
            //
            // Finish the current night.
            //
            night.finish();
        }
    };
    //
    // Display Mode.
    //
    $(".displayMode").change(function() {
        return Status.displayAll = $("input[name=displayMode]:checked").val() === "displayAll";
    }).change(display);
    //
    // Dynamically generate year selection.
    //
    var years = function() {
        var firstYear = 2018;
        var d = new Date();
        var currentYear = d.getFullYear();
        $("#years").empty();
        $("#years").append($("<legend/>").html("Display Year"));
        for (var year = currentYear; year >= firstYear; year--) {
            var d = $("<div/>", {"class": "form-check form-check-inline"});
            var io = {"class": "form-check-input displayYear", "type": "radio", "name": "displayYear", "id": "display" + year, "value": "" + year}
            if (year == currentYear) io["checked"] = "checked";
            var i = $("<input/>", io);
            var l = $("<label/>", {"class": "form-check-label displayYear", "for": "display" + year}).html("" + year);
            d.append(i);
            d.append(l);
            $("#years").append(d);
        }
    };
    years();
    $(".displayYear").change(function() {
        Status.displayYear = $("input[name=displayYear]:checked").val();
        if (Status.raw.hasOwnProperty(Status.displayYear)) {
            display();
        } else {
            Status.nights[Status.displayYear] = [];
            $.getJSON(Status.dataFile(), {}, function(data) {Status.raw[Status.displayYear] = data;}).done(display);
        }
        return true;
    });
    //
    // Load initial data.
    //
    $.getJSON(Status.dataFile(), {}, function(data) {Status.raw[Status.displayYear] = data;}).done(display);
    return true;
});
