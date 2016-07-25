/*
 * Copyright (c) 2015, Meridian Technologies, Inc.  All rights reserved. 
 * Created by tdockins on 7/22/2016.
 */
var util      = require( "util" );
var os        = require( "os" );
var eol       = os.EOL;
var Transform = require( "stream" ).Transform;

function Transformer( params, options ) {
    if ( !(this instanceof Transformer) )
        return new Transformer( options );
    Transform.call( this, options );
    var _param = {
        fieldDefinitions      : null,
        skipFirstLine         : false,
        acctCodeToIndustryMap : null,
        responseCodeToNameMap : null,
        noOutput              : false
    };
    if ( params && typeof params === "object" ) {
        for ( var key in params ) {
            if ( params.hasOwnProperty( key ) ) {
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

}

util.inherits( Transformer, Transform );

Transformer.prototype._transform = function ( chunk, encoding, callback ) {
    if (this.params.noOutput) {
        callback();
    } else {
        var data = JSON.parse( chunk.toString() );
        try {


            /*
             * If dispute code 2 is not empty, it needs to be counted separately.  So, we put the code
             * into a standardized column for dispute codes, copy and transform the data,
             * and push it into the stream separately.
             */

            if ( data["DISPUTE_CODE_2"] != '' ) {
                var newData = JSON.parse( chunk.toString() );
                this.push( JSON.stringify( this.mapping( newData, true) ) );
            }

            this.push( JSON.stringify( this.mapping( data ) ) );
            callback(); // move on
        } catch ( e ) {
            this.emit( "error", e );
        }
    }


};

Transformer.prototype._flush = function ( cb ) {
    this.push( null );
    cb();
};


Transformer.prototype.mapping = function ( data, two ) {
    if ( two ) {
        data["DISPUTE_CODE"]  = data["DISPUTE_CODE_2"];
    } else {
        data["DISPUTE_CODE"]  = data["DISPUTE_CODE_1"];
    }
    data["INDUSTRY_NAME"] = this.params.acctCodeToIndustryMap[data["ACCT_TYPE_CODE"]];

    if (this.params.responseCodeToNameMap) {
        data["RESPONSE_NAME"] = this.params.responseCodeToNameMap[ data[ "ACDV_RESPONSE_CODE" ] ];
    }

    return data;
};

module.exports = Transformer;