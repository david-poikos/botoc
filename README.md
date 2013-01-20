botoc
=====

C++ wrapper for the boto (Amazon Web Services) Python library contained within
simple headers; nothing to link.

Requirements
------------

* Python libraries (python-dev)
* Boto 2.6+ (easy-install boto)
* AWS account with SQS or DDB set up, and an IAM user with appropriate
  permissions

Functionality
-------------

Provides very limited (but very easy to use) functionality for AWS services:

Common
* botoc::set_iam_user Change the IAM credentials (key and secret)
* botoc::set_region Change the working region (e.g. "eu-west-1")

SQS (botoc_sqs.h)
* botoc::sqs::prep Prepare a connection with the current region and credentials
  (called automatically when needed). Connections will persist until disconnect
  is called.
* botoc::sqs::put Adds a new item to the queue.
* botoc::sqs::get Gets an item from the queue.
  * supports long-polling (see BOTO_SUPPORTS_WAIT_TIME_SECONDS comment).
* botoc::sqs::remove Removes an item from the queue using a handle from sqs_get.
* botoc::sqs::disconnect Breaks the current connection; only needed for
  reconnecting as a different user or region.

DDB (botoc_ddb.h)
* botoc::ddb::prep Prepare a connection with the current region and credentials
  (called automatically when needed). Connections will persist until disconnect
  is called.
* botoc::ddb::update Adds or Updates an item in the database
  * supported types: string (S), number (N), binary (B) and sets of each
  * supports "expected"
  * supports "PUT", "ADD", "DELETE"
  * does *not* support range keys
* botoc::ddb::get Retrieves an item from the database
  * supports full & partial get
  * does *not* support metadata
  * does *not* support range keys

Examples
--------

See main.cpp for example usage.

Credits
-------

Based on https://github.com/9apps/dynamoDBc
