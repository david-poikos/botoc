botoc
=====

C++ wrapper for the boto (Amazon Web Services) Python library contained within a single header.

Requirements
------------

* Python libraries (python-dev)
* Boto 2.6+ (easy-install boto)
* AWS account with SQS or DDB set up, and an IAM user with appropriate permissions

Functionality
-------------

Provides very limited (but very easy to use) functionality for the SQS and DDB services:

SQS
* sqs_put Adds a new string to the queue
* sqs_get Gets a string from the queue
  * supports long-polling (uncomment if boto version is sufficient)
* sqs_delete Removes a string from the queue using a handle from sqs_get

DDB
* ddb_update Adds or Updates an item in the database
  * supported types: string (S), number (N), binary (B)
  * does *not* support lists
  * supports "expected"
  * supports "PUT", "ADD", "DELETE"
  * does *not* support range keys
* ddb_get Retrieves an item from the database
  * supports full & partial get
  * does *not* support metadata
  * does *not* support range keys

Examples
--------

See main.cpp for example usage.

Credits
-------

Based on https://github.com/9apps/dynamoDBc
