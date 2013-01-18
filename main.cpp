// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

#define AWS_REGION "eu-west-1"
#define AWS_KEYID "your key here"
#define AWS_SECRET "your key secret here"

/* You should have an IAM Policy for the user above which is similar to this:
{
	"Statement": [
	{
		"Sid": "MyQueuePermissions",
		"Action": [
			"sqs:DeleteMessage",
			"sqs:GetQueueAttributes",
			"sqs:GetQueueUrl",
			"sqs:ReceiveMessage",
			"sqs:SendMessage"
		],
		"Effect": "Allow",
		"Resource": [
			"arn:aws:sqs:eu-west-1:*:mytestqueue"
		]
	},
	{
		"Sid": "MyDatabasePermissions",
		"Action": [
			"dynamodb:GetItem",
			"dynamodb:UpdateItem"
		],
		"Effect": "Allow",
		"Resource": [
			"arn:aws:dynamodb:eu-west-1:*:table/mytestdatabase"
		]
	}
	]
}
 */

#include <Python/python.h>
//#include <python2.7/Python.h>

#include "aws_static.h"

#include <stdio.h>

void print_key_values( FILE *fp, const std::vector<ddb_item> &items );
void print_keys( FILE *fp, const std::vector<ddb_item> &items );

void test_sqs( void );
void test_ddb( void );

void print_key_values( FILE *fp, const std::vector<ddb_item> &items ) {
	if( items.size( ) > 0 ) {
		fprintf( fp, "[\n" );
		for( std::size_t i = 0, e = items.size( ); i < e; ++ i ) {
			fprintf( fp, "  \"%s\" => [%c]: \"%.*s\"\n", items[i].name( ).c_str( ), DDBTypeS[items[i].type()], (int) items[i].value_length( ), items[i].value( ) );
		}
		fprintf( fp, "]" );
	} else {
		fprintf( fp, "[]" );
	}
}

void print_keys( FILE *fp, const std::vector<ddb_item> &items ) {
	if( items.size( ) > 0 ) {
		fprintf( fp, "[\n" );
		for( std::size_t i = 0, e = items.size( ); i < e; ++ i ) {
			fprintf( fp, "  \"%s\"\n", items[i].name( ).c_str( ) );
		}
		fprintf( fp, "]" );
	} else {
		fprintf( fp, "[]" );
	}
}

void test_sqs( void ) {
	fprintf( stdout, "begin SQS.\n" );
	
	std::string queue( "mytestqueue" );
	std::string msg;
	
	void *handle;
	std::string body;
	fprintf( stdout, "sqs_get\n" );
	if( sqs_get( queue, &handle, body, 10, 4 ) ) {
		fprintf( stdout, "  ok. result = %s\n", body.c_str( ) );
		fprintf( stdout, "sqs_delete( \"%s\", &handle )\n", queue.c_str( ) );
		if( sqs_delete( queue, &handle ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
	} else {
		fprintf( stdout, "  nothing.\n" );
	}
	
	msg = "Hello World";
	
	fprintf( stdout, "sqs_put\n" );
	if( sqs_put( queue, msg ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	
	fprintf( stdout, "done SQS.\n\n" );
	fflush( stdout );
}

void test_ddb( void ) {
	fprintf( stdout, "begin DDB.\n" );
	
	std::string database( "mytestdatabase" );
	std::string key;
	std::vector<ddb_item> items;
	std::vector<ddb_item> expect;
	
	
	key = "mykey2";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", true, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, true, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	
	key = "mykeyGone";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", true, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, true, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	
	key = "mykey1";
	float five = 5;
	{ ddb_item itm( "Name", "Fred" ); items.push_back( itm ); }
	{ ddb_item itm( "Age", 32 ); items.push_back( itm ); }
	{ ddb_item itm( "Data", &five, sizeof( five ), DDB_BINARY ); items.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	
	key = "mykey1";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", true, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, true, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey1";
	{ ddb_item itm( "Age" ); items.push_back( itm ); }
	{ ddb_item itm( "Data" ); items.push_back( itm ); }
	{ ddb_item itm( "What" ); items.push_back( itm ); }
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", true, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, true, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	float count[10] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	{ ddb_item itm( "Name", "John" ); items.push_back( itm ); }
	{ ddb_item itm( "Age", 28 ); items.push_back( itm ); }
	{ ddb_item itm( "Data", &count, sizeof( count ), DDB_BINARY ); items.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	{ ddb_item itm( "Age", 25 ); items.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", true, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, true, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	{ ddb_item itm( "Age", 30 ); items.push_back( itm ); }
	{ ddb_item itm( "Age", 25 ); expect.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, ", &" );
	print_key_values( stdout, expect );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items, &expect ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	{ ddb_item itm( "Age", 40 ); items.push_back( itm ); }
	{ ddb_item itm( "Age", 25 ); expect.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, ", &" );
	print_key_values( stdout, expect );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items, &expect ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	{ ddb_item itm( "Age", 50 ); items.push_back( itm ); }
	{ ddb_item itm( "Age", 10 ); expect.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, ", &" );
	print_key_values( stdout, expect );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items, &expect ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", false, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, false, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	fprintf( stdout, "sleep( 1 )\n" );
	sleep( 1 );
	
	key = "mykey2";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", false, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, false, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	{ ddb_item itm( "Age", 100 ); items.push_back( itm ); }
	
	fprintf( stdout, "ddb_update( \"%s\", \"%s\", ", database.c_str( ), key.c_str( ) );
	print_key_values( stdout, items );
	fprintf( stdout, ", &" );
	print_key_values( stdout, expect );
	fprintf( stdout, " ):\n" );
	if( ddb_update( database, key, items, &expect ) ) {
		fprintf( stdout, "  ok.\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	key = "mykey2";
	
	fprintf( stdout, "ddb_get( \"%s\", \"%s\", false, ", database.c_str( ), key.c_str( ) );
	print_keys( stdout, items );
	fprintf( stdout, " ):\n" );
	if( ddb_get( database, key, false, items ) ) {
		fprintf( stdout, "  ok. values = " );
		print_key_values( stdout, items );
		fprintf( stdout, "\n" );
	} else {
		fprintf( stdout, "  fail.\n" );
	}
	items.clear( );
	expect.clear( );
	fprintf( stdout, "\n" );
	
	fprintf( stdout, "done DDB.\n\n" );
	fflush( stdout );
}

int main( void ) {
	fprintf( stdout, "begin.\n\n" );
	
	test_sqs( );
	test_ddb( );
	
	fprintf( stdout, "done.\n\n" );
	fflush( stdout );
	
    return 0;
}
