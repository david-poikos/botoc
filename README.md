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
* SQS
  * sqs_put Adds a new string to the queue
  * sqs_get Gets a string from the queue (optionally with long polling)
  * sqs_delete Removes a string from the queue using a handle from sqs_get
* DDB
  * ddb_update Adds or Updates an item in the database
    (supports string, number, binary types, supports "expected", does not support "add")
  * ddb_get Retrieves an item (optionally only specified attributes) from the database

Examples
--------

See main.cpp for example usage.

Credits
-------

Based on https://github.com/9apps/dynamoDBc
