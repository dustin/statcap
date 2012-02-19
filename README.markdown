# Capture memcached Stats

There are millions of these things, and I've written several myself.
This is another one.

The purpose of this tool is to capture stats from a
memcached/membase/couchbase node and store them into a CouchDB for
offline analysis.  This is suitable for unattended operation at a
customer site reporting back home.

# Usage

    Usage of ./statcap:
      -out="http://localhost:5984/stats": http://couch.db/path or a /file/path
      -proto="": Proto document, into which timings stats will be added
      -server="localhost:11211": memcached server to connect to
      -sleep=5: Sleep time between samples
      -stats="timings,kvtimings": stats to fetch beyond toplevel; comma separated

Most of the usage should be obvious, but I'll add even more
description here so people know what's up.

## Out

Where to send the output.  This can be a URL to a CouchDB, or a file
path where a stream of gzipped JSON docs will be written.

CouchDB supports basic auth, so you can do something like this:

    ./statscap -out=http://myuser:mypassword@me.iriscouch.com/secretdb

or a plain file:

    ./statscap -out=file.gz

## Proto

This one probably needs the most explanation, but the rationale is
pretty straightforward:  I want to be able to log additional
properties along with the stat snapshots so I can aggregate the
records more easily.

In order to do this, I supply a JSON document prototype that has a
bunch of properties, and then build the stats on top of it.  For
example:

    {
        "customer": "enterprise rent a bar",
        "mission_date": "2012-02-18T13:30:00",
        "attempt": 382
    }

Those properties will now appear in the final documents.

If your proto document includes properties that match the names of
stats you're capturing, the stats will win.  Don't do that and expect
otherwise.

## Server

The memcached server to talk to.  Binary protocol only and no auth
support currently.

## Sleep

How long (in seconds) to wait between samples.

## Stats

The toplevel stats are always captured and stored as "`all`".
You can specify additional stats to grab here.  The default list
includes some I care about right now and might change in the future.
