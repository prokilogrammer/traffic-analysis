var moment = require('moment');
var mongojs = require('mongojs');
var async = require('async');
var _ = require('lodash');
var querystring = require('querystring');
var request = require('request');

var dbUrl = "mongodb://127.0.0.1:27017/trafficdb";
if (process.env.MONGOHQ_URL)
    dbUrl = process.env.MONGOHQ_URL

console.log("Connecting to " + dbUrl);
var db = mongojs(dbUrl);
var configCollection = db.collection('traffic.config');
var dataCollection = db.collection('traffic.data');

var wsdotAccessId = "bb417a8c-d50e-41fb-a1ac-61a101f2a701"
var allFlowsBase = "http://www.wsdot.wa.gov/Traffic/api/TrafficFlow/TrafficFlowREST.svc/GetTrafficFlowsAsJson"
var singleFlowBase = "http://www.wsdot.wa.gov/Traffic/api/TrafficFlow/TrafficFlowREST.svc/GetTrafficFlowAsJson" 

async.waterfall([

        // 1. Get the config
        function(cb){

            configCollection.find({}).limit(1).toArray(function(err, docs){
                if (err) throw(err);

                var config = null;
                if (docs.length > 0)
                    config = docs[0];

                cb(null, config);
            });
        },

        // 2. Based on the config, grab traffic data
        function(config, cb){

            var urls = []
            if (_.isNull(config) || !_.has(config, 'flowIds')){
                // Grab the entire traffic information 
                urls.push(allFlowsBase + '?' + querystring.stringify({AccessCode: wsdotAccessId}));
            }
            else {

                _.forEach(config.flowIds, function(flowId){
                    urls.push(singleFlowBase + '?' + querystring.stringify({AccessCode: wsdotAccessId, FlowDataID: flowId})); 
                });
            }

            cb(null, urls);
        },


        // 3. Grab data from supplied urls
        function(urls, cb){

            var responses = []
            async.each(urls, function(url, eachcb){
                console.log("Fetching " + url);
                request.get(url, function(err, resp, body){
                    if (!err && resp.statusCode == 200){
                        _.forEach(JSON.parse(body), function(ele){ responses.push(ele)});
                        eachcb();
                    }
                    else {
                        eachcb(err);
                    }
                });
            },
            
            function(err){cb(err, responses)}
            );
        },

        // 4. Insert the extracted entries into db
        function(responses, cb){

            var dateRe = /(\d+)([-+]\d{0,4})?/ 
            console.log("Inserting " + responses.length + " new entries to db");
            async.each(responses, function(resp, eachcb){

                var time = resp.Time;
                if (dateRe.test(time)){
                    match = time.match(dateRe)
                    var ts = _.parseInt(match[1]);
                    var zoneOffset = match[2];
                    var momentTime = moment(ts);
                    if (zoneOffset) { momentTime = momentTime.zone(zoneOffset) }
                    resp.timestamp = momentTime.unix();
                    resp.zoneOffset = zoneOffset;
                }

                dataCollection.update({FlowDataID: resp.FlowDataID, Time: resp.Time}, {'$setOnInsert': resp}, {upsert: true}, function(err){eachcb(err)});
            },

            function(err) {cb(err)}
            );
        },
        ],

        function(err){
            if (err) throw(err); 
            process.exit(0);
        }
        );

