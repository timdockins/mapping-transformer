/*
 * Copyright (c) 2015, Meridian Technologies, Inc.  All rights reserved. 
 * Created by tdockins on 7/22/2016.
 */
var util      = require( "util" );
var Transform = require( "stream" ).Transform;
var fs        = require( "fs" );

var moment = require( "moment" );
var lodash = require( "lodash" );

function Profiler( params, options ) {



    this.dataProfile = {};

    if ( !(this instanceof Profiler) )
        return new Profiler( options );
    Transform.call( this, options );
    var _param = {
        fieldDefinitions : null,
        outputFilename   : "data_profile.txt",
        firstRowIsHeader : true,
        constructResult  : false
    };
    if ( params && typeof params === "object" ) {
        for ( var key in params ) {
            if ( params.hasOwnProperty() ) {
                //noinspection JSUnfilteredForInLoop
                _param[key] = params[key];
            }
        }
    }


    this.performProfile  = true;
    this._options        = options || {};
    //noinspection JSUnresolvedVariable
    this.params          = _param;
    //noinspection JSUnresolvedVariable
    this.params._options = this._options;
    this.params.fieldDefinitions = params.fieldDefinitions;

    if ( this.params.fieldDefinitions === null ) {
        console.warn( "Profiler requires that fields are defined.  See usage details in the package readme file." );
        this.performProfile = false;
    } else {
        this.dataProfile["totalCount"] = 0;
        this.params.fieldDefinitions.forEach( function ( cV ) {
            this.dataProfile[cV.name] = {
                valuesFound        : [],
                invalidValuesFound : []
            }
        }, this );
    }

    this.outputFile = fs.createWriteStream( this.params.outputFilename );

    this.on( "end", function () {
        var profiled = this.profileData();
        this.outputFile.write(JSON.stringify(profiled), null,  2);
        this.outputFile.end();
    } );

    return this;


}
util.inherits( Profiler, Transform );

Profiler.prototype._transform = function ( chunk, encoding, callback ) {
    var processCursorChars = ['-', '|', '/'];
    var data = JSON.parse( chunk.toString());
    try {
        if ( this.dataProfile.totalCount == 0 ) {
            this.outputFile.write( "//ACDV Record Profiler ************************************\n" );
        }
        if ( this.dataProfile.totalCount < 2 ) {
            this.outputFile.write( chunk.toString() );
        }

        if ( this.performProfile ) {
            this.params.fieldDefinitions.forEach( function ( cV ) {
                if ( this.dataProfile.hasOwnProperty( cV.name ) && data.hasOwnProperty( cV.name ) ) {
                    this.dataProfile[cV.name].valuesFound.push( data[cV.name].toString() );
                    if ( !this.isValid( data[cV.name], cV ) ) {
                        this.dataProfile[cV.name].invalidValuesFound.push( data[cV.name].toString() );
                    }
                }
            }, this );
        }
        this.dataProfile.totalCount += 1;
        process.stdout.clearLine();  // clear current text
        process.stdout.cursorTo(0);  // move cursor to beginning of line
        process.stdout.write('Record # ' + this.dataProfile.totalCount.toString());

        callback( null, chunk );
    } catch ( e ) {
        console.error( "acdv Profiler error", chunk.toString() );
        console.error( e.stack );
        //noinspection JSUnresolvedFunction
        this.emit( "error", e );
    }


};

Profiler.prototype._flush = function ( cb ) {
    this.push( null );
    cb();
};

Profiler.prototype.isValid = function ( cv, definition ) {
    //noinspection JSUnresolvedFunction
    if ( definition.type === 'DATE' && !moment( cv, definition.format ).isValid() ) {
        return false;
    }
    if ( definition.hasOwnProperty( 'restrictedValues' ) && definition.restrictedValues ) {
        if ( lodash.indexOf( definition.restrictedValues, cv ) == -1 ) {
            return false;
        }
    }
    return true;
};

Profiler.prototype.profileData = function () {
    this.params.fieldDefinitions.forEach(function (field) {
        if (this.dataProfile.hasOwnProperty(field.name) && this.dataProfile[ field.name ].hasOwnProperty( "valuesFound" )) {

            this.dataProfile[ field.name ].valuesFound = lodash.uniq( this.dataProfile[ field.name ].valuesFound );
            if ( this.dataProfile[ field.name ].valuesFound.length > 100 ) {
                this.dataProfile[ field.name ].valuesFound = lodash.sampleSize( this.dataProfile[ field.name ].valuesFound, 20 );
                this.dataProfile[ field.name ]["sampled"] = true;
            }
            this.dataProfile[ field.name ].uniqueValuesCount = this.dataProfile[ field.name ].valuesFound.length;
        }

    }, this);
    return this.dataProfile;
};

module.exports = Profiler;