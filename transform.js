/*
 * Copyright (c) 2015, Meridian Technologies, Inc.  All rights reserved. 
 * Created by tdockins on 7/22/2016.
 */
"use strict";
var csvtojson = require( "csvtojson" );
var jsontocsv = require( "json2csv-stream" );
var fs        = require( "fs" );
var moment    = require( "moment" );
//noinspection JSUnresolvedVariable
var argv      = require( "yargs" ).argv;
var eol       = require( "os" ).EOL;

var industryMappings = require( "./industryMappings.json" );
var fieldDefinitions = require( "./fieldDefinitions.json" );

var Profiler    = require( "./lib/core" ).Profiler;
var Transformer = require( "./lib/core" ).Transformer;

(function () {
    var csvToJsonConverter = new csvtojson.Converter( {checkType : false} );


    var showHeader         = argv.showHeader ? argv.showHeader : false;
    var jsonToCsvConverter = new jsontocsv( {
        del        : ",",
        showHeader : showHeader
    } );

    //noinspection JSUnresolvedVariable
    if ( !argv.i ) {
        console.log( 'Filename not defined.  Please define input file with --i="<filename>" argument' );
        process.exit();
    }

    //noinspection JSUnresolvedVariable
    var inFilename  = argv.i;
    //noinspection JSUnresolvedVariable
    var outFilename = argv.o || 'all.csv';

    var outStream = fs.createWriteStream( outFilename, {'flags' : 'a'} );
    var inStream  = fs.createReadStream( inFilename );

    var profiler = new Profiler( {
        fieldDefinitions : fieldDefinitions,
        outputFilename   : argv.p || null
    }, {} );


    var noOutput = !!argv.noOutput;

    var transformer = new Transformer( {
        acctCodeToIndustryMap : industryMappings,
        noOutput              : noOutput
    } );


    if ( argv.p ) {
        inStream
            .pipe( csvToJsonConverter )
            .pipe( profiler )
            .pipe( transformer )
            .pipe( jsonToCsvConverter )
            .pipe( outStream );
    } else {
        inStream
            .pipe( csvToJsonConverter )
            .pipe( transformer )
            .pipe( jsonToCsvConverter )
            .pipe( outStream );
    }

    outStream.on( "finish", function () {
        console.log( "All finished." );
        try {
            var outfile = fs.createWriteStream( outFilename, {'flags' : 'a'} );
            outfile.write( "\n" );
            outfile.end();
        } catch ( e ) {
            console.error( "could not write to " + outFilename );
            this.emit( "error", e );
        }
    } );


})();