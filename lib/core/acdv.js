"use strict";
var csvtojson = require("csvtojson");
var jsontocsv = require("jsontocsv");
var fs = require("fs");
var _ = require("lodash");
var moment = require("moment");
var argv = require("yargs").argv;

var Transform = require("stream").Transform;
var Readable = require("stream").Readable;
var Result = require("./Result");

function profile() {
 
    var converter = new csvtojson.Converter({checkType : false});

    var recordTemplate = [
        {
            name : "CRA_ACDV_CONTROL_NUM",
            type : "STRING",
            restrictedValues : false,
            emptyAllowed : false
        },
        {
            name : "ORIGINATOR_CODE",
            type : "STRING",
            restrictedValues : ['EFX', 'ESP', 'TUN'],
            emptyAllowed : false
        },
        {
            name : "REQUEST_SUBMITTED_DATE",
            type : "DATE",
            format : "DD-MMM-YY HH:mm:ss",
            emptyAllowed : false
        },
        {
            name : "RESPONSE_DUE_DATE",
            type : "DATE",
            format : "DD-MMM-YY HH:mm:ss",
            emptyAllowed : false
        },
        {
            name : "STATUS_CODE",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "STATUS_NAME",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "STATUS_DATE",
            type : "DATE",
            format : "DD-MMM-YY HH:mm:ss",
            emptyAllowed : true
        },
        {
            name : "ACDV_RESPONSE_CODE",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "DISPUTE_CODE_1",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "DISPUTE_CODE_2",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "IMAGE_COUNT",
            type : "NUMBER",
            emptyAllowed : false
        },
        {
            name : "IMAGE_RECEIVED_COUNT",
            type : "STRING",
            emptyAllowed : true
        },
        {
            name : "ACCT_TYPE_CODE",
            type : "STRING",
            emptyAllowed : false,
            restrictedValues : [
                '00',
                '3A',
                '48',
                '0C',
                '77',
                '18',
                '0G',
                '2A',
                '8A',
                '37',
                '50',
                '93',
                '94',
                '65','66','67','68','69','70','71','72','73','74','75',
                '01','02','03','06','10','11','13','20','29','90','91','95','6A','7B','8B','0F',
                '21','22','23','24','34','78','1A','1C',
                '04','05','08','17','19','25','26','89','0A','5A','9A','5B','6B','2C','6D','27','85','87',
                '15','43','47','7A','9B','07','12','92','4D',
                '31',''
            ]
        }

    ];
    

    if ( !argv.i ) {
        console.log('Filename not defined.  Please define input file with --i="<filename>" argument' );
        process.exit();
    }
    var filename = argv.i;

    converter.fromFile(filename, function(err, result) {
        if (!err) {
            console.log('Retrieved ' + result.length + ' records. ');
            console.log('First 2 records');
            console.log( result[0]);
            console.log( result[1]);
        }        
        _.forEach( recordTemplate, function( key ) {
            console.log('****************************************************************');
            console.log('Property: ' + key.name + ' should be : ' + key.type );
            var uniqValues = _.map(_.uniqBy( result, key.name ), function( cv ) {
                if ( key.type == "DATE" && !moment(cv[ key.name ], key.format).isValid() ) {
                    console.log(cv[key.name] + " is an invalid date");
                }
                if (key.hasOwnProperty('restrictedValues') && key.restrictedValues) {
                    if (_.indexOf(key.restrictedValues, cv[ key.name ]) == -1 ) {
                        console.log('INVALID VALUE: ' + cv[key.name]);
                    }
                }
                return cv[ key.name ];
            });
            console.log(uniqValues.length + ' unique values.');
            if (uniqValues.length < 200) {
                console.log( uniqValues );                
            } 
        });
    });
}
module.exports.profile = profile;

function combine( industryMappings ) {
        
    var csvToJsonConverter = new csvtojson.Converter({checkType : false});

    csvToJsonConverter.transform = function( json, row, index ) {
        if (json.hasOwnProperty('ACCT_TYPE_CODE') && json.ACCT_TYPE_CODE != '') {
            json["INDUSTRY"] = industryMappings[ json.ACCT_TYPE_CODE ];
        } else {
            json["INDUSTRY"] = "UNSPECIFIED";
        }
    }
 

    var jsonToCsvConverter = new jsontocsv({
        del : ",",
        showHeader : argv.showHeader
    });

    if ( !argv.i ) {
        console.log('Filename not defined.  Please define input file with --i="<filename>" argument' );
        process.exit();
    }
    
    var inFilename = argv.i;
    var outFilename = argv.o || 'all.csv';

    var outStream = fs.createWriteStream(outFilename, {'flags':'a'});
    var inStream = fs.createReadStream(inFilename);

    inStream.pipe(csvToJsonConverter).pipe(jsonToCsvConverter).pipe(outStream);

}

module.exports.combine = combine;