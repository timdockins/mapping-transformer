# mapping-transformer

Validate, transform, and combine comma-delimited files.

This utility can process comma-delimited text files and map existing values to new values on a record-by-record basis.  It is essentially a custom implementation of a NodeJS Transform.

Mappings are based on a simple configuration written in a JSON configuration file.


# Installation

## Preparation

* Make sure you have NodeJS installed.  You can [download it here](https://nodejs.org/en/).
* Unzip these files to an empty directory.

## Installation

Change directory to the directory containing the unzipped files.  Then, with a connection to the internet, run the following command.

>npm install

# Usage

Run the following for each file that you want to import.  The input file will be processed and concatenated to the end of the output file.

> node transform --i="inputfilename.csv" --o="outputfilename.csv"

To profile/validate the data add the --p="profile_textfilename.txt" flag.  For example, you could use:

> node transform --i="inputfilename.csv" --o="outputfilename.csv" --p="inputfilename.txt"
