// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

// must include python before botoc
#include <Python/python.h>
//#include <python2.7/Python.h>

#include "botoc_sqs.h"
#include "botoc_ddb.h"

#include <stdio.h>


/* prototypes */

void print_key_values( FILE *fp, const botoc::ddb::item_list_t &items ) throw( );
void print_keys( FILE *fp, const botoc::ddb::item_list_t &items ) throw( );

void test_sqs( const botoc::string_t &queue ) throw( );
void test_ddb( const botoc::string_t &database ) throw( );


/* implementation */

int main( void ) {
	fprintf( stdout, "begin.\n\n" );
	
	botoc::set_region( "eu-west-1" );
	botoc::set_iam_user( "user_key_here", "user_secret_here" );
	
	/* You should have an IAM Policy for the user above which is similar to this:
	 * {
	 * 	"Statement": [
	 * 	{
	 * 		"Sid": "MyQueuePermissions",
	 * 		"Action": [
	 * 			"sqs:DeleteMessage",
	 * 			"sqs:GetQueueAttributes",
	 * 			"sqs:GetQueueUrl",
	 * 			"sqs:ReceiveMessage",
	 * 			"sqs:SendMessage"
	 * 		],
	 * 		"Effect": "Allow",
	 * 		"Resource": [
	 * 			"arn:aws:sqs:eu-west-1:*:mytestqueue"
	 * 		]
	 * 	},
	 * 	{
	 * 		"Sid": "MyDatabasePermissions",
	 * 		"Action": [
	 * 			"dynamodb:GetItem",
	 * 			"dynamodb:UpdateItem"
	 * 		],
	 * 		"Effect": "Allow",
	 * 		"Resource": [
	 * 			"arn:aws:dynamodb:eu-west-1:*:table/mytestdatabase"
	 * 		]
	 * 	}
	 * 	]
	 * }
	 */
	
	test_sqs( "mytestqueue" );
	test_ddb( "mytestdatabase" );
	
	fprintf( stdout, "done.\n\n" );
	fflush( stdout );
	
    return 0;
}

void test_sqs( const botoc::string_t &queue ) throw( ) {
	fprintf( stdout, "begin SQS.\n" );
	
	LOCALBLOCK {
		fprintf( stdout, "botoc::sqs::get( \"%.*s\", body, 10, 4 )\n", SIZED_STRING(queue) );
		botoc::string_t body;
		botoc::handle_t handle = botoc::sqs::get( queue, body, 10, 4 );
		if( handle != NULL ) {
			fprintf( stdout, "  ok. result = %.*s\n", SIZED_STRING(body) );
			fprintf( stdout, "botoc::sqs::remove( \"%.*s\", &handle )\n", SIZED_STRING(queue) );
			if( botoc::sqs::remove( queue, handle ) ) {
				fprintf( stdout, "  ok.\n" );
			} else {
				fprintf( stdout, "  fail.\n" );
			}
		} else {
			fprintf( stdout, "  nothing.\n" );
		}
	}
	
	LOCALBLOCK {
		fprintf( stdout, "botoc::sqs::put( \"%.*s\", \"Hello World\" )\n", SIZED_STRING(queue) );
		if( botoc::sqs::put( queue, "Hello World" ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
	}
	
	fprintf( stdout, "done SQS.\n\n" );
	fflush( stdout );
}

void test_ddb( const botoc::string_t &database ) throw( ) {
	fprintf( stdout, "begin DDB.\n" );
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey2\", true, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey2", true, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykeyGone\", true, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykeyGone", true, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Name", "Fred" ) );
		items.push_back( botoc::ddb::item( "Age", 32 ) );
		float five = 5;
		items.push_back( botoc::ddb::item( "Data", &five, sizeof( five ), botoc::ddb::BINARY ) );
		items.push_back( botoc::ddb::item( "Friends", botoc::ddb::STRINGSET ) );
		items.back( ).add_item( "Bob" );
		items.back( ).add_item( "Bill" );
		items.push_back( botoc::ddb::item( "Pets", botoc::ddb::STRINGSET ) );
		items.back( ).add_item( "Poochey" );
		items.back( ).add_item( "Tiddles" );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey1\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey1", items ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Friends", botoc::ddb::STRINGSET, botoc::ddb::ADD ) );
		items.back( ).add_item( "Tiddles" );
		items.push_back( botoc::ddb::item( "Pets", botoc::ddb::STRINGSET, botoc::ddb::DELETE ) );
		items.back( ).add_item( "Tiddles" );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey1\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey1", items ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey1\", true, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey1", true, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Age" ) );
		items.push_back( botoc::ddb::item( "Data" ) );
		items.push_back( botoc::ddb::item( "What" ) );
		
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey1\", true, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey1", true, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		float count[10] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		items.push_back( botoc::ddb::item( "Name", "John" ) );
		items.push_back( botoc::ddb::item( "Age", 28 ) );
		items.push_back( botoc::ddb::item( "Data", &count, sizeof( count ), botoc::ddb::BINARY ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Age", 25 ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Age", 1, botoc::ddb::ADD ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey2\", true, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey2", true, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		botoc::ddb::item_list_t expect;
		items.push_back( botoc::ddb::item( "Age", 30 ) );
		expect.push_back( botoc::ddb::item( "Age", 26 ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, ", &" );
		print_key_values( stdout, expect );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items, &expect ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		botoc::ddb::item_list_t expect;
		items.push_back( botoc::ddb::item( "Age", 40 ) );
		expect.push_back( botoc::ddb::item( "Age", 26 ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, ", &" );
		print_key_values( stdout, expect );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items, &expect ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		botoc::ddb::item_list_t expect;
		items.push_back( botoc::ddb::item( "Age", 50 ) );
		expect.push_back( botoc::ddb::item( "Age", 10 ) );
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, ", &" );
		print_key_values( stdout, expect );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items, &expect ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey2\", false, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey2", false, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	fprintf( stdout, "sleep( 1 )\n" );
	sleep( 1 );
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey2\", false, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey2", false, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		items.push_back( botoc::ddb::item( "Age", 100 ) );
		botoc::ddb::item_list_t expect;
		
		fprintf( stdout, "botoc::ddb::update( \"%.*s\", \"mykey2\", ", SIZED_STRING(database) );
		print_key_values( stdout, items );
		fprintf( stdout, ", &" );
		print_key_values( stdout, expect );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::update( database, "mykey2", items, &expect ) ) {
			fprintf( stdout, "  ok.\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	LOCALBLOCK {
		botoc::ddb::item_list_t items;
		fprintf( stdout, "botoc::ddb::get( \"%.*s\", \"mykey2\", false, ", SIZED_STRING(database) );
		print_keys( stdout, items );
		fprintf( stdout, " ):\n" );
		if( botoc::ddb::get( database, "mykey2", false, items ) ) {
			fprintf( stdout, "  ok. values = " );
			print_key_values( stdout, items );
			fprintf( stdout, "\n" );
		} else {
			fprintf( stdout, "  fail.\n" );
		}
		fprintf( stdout, "\n" );
	}
	
	fprintf( stdout, "done DDB.\n\n" );
	fflush( stdout );
}

/* helper functions */

void print_key_values( FILE *fp, const botoc::ddb::item_list_t &items ) throw( ) {
	if( items.size( ) > 0 ) {
		fprintf( fp, "{\n" );
		for( std::size_t i = 0, e = items.size( ); i < e; ++ i ) {
			if( items[i].type( ) == botoc::ddb::UNKNOWN ) {
				fprintf( fp, "  \"%.*s\" => ?\n", SIZED_STRING(items[i].name( )) );
			} else if( (items[i].type( ) & botoc::ddb::SET) ) {
				fprintf( fp, "  \"%.*s\" => [%s] (%s): [\n", SIZED_STRING(items[i].name( )), items[i].type_string( ), items[i].action_string( ) );
				const botoc::string_list_t &o = items[i].list_knowntype( );
				for( std::size_t j = 0, f = o.size( ); j < f; ++ j ) {
					fprintf( fp, "    \"%.*s\"\n", SIZED_STRING(o[j]) );
				}
				fprintf( fp, "  ]\n" );
			} else {
				fprintf( fp, "  \"%.*s\" => [%s] (%s): \"%.*s\"\n", SIZED_STRING(items[i].name( )), items[i].type_string( ), items[i].action_string( ), SIZED_STRING(items[i].value_knowntype( )) );
			}
		}
		fprintf( fp, "}" );
	} else {
		fprintf( fp, "{}" );
	}
}

void print_keys( FILE *fp, const botoc::ddb::item_list_t &items ) throw( ) {
	if( items.size( ) > 0 ) {
		fprintf( fp, "[\n" );
		for( std::size_t i = 0, e = items.size( ); i < e; ++ i ) {
			fprintf( fp, "  \"%.*s\"\n", SIZED_STRING(items[i].name( )) );
		}
		fprintf( fp, "]" );
	} else {
		fprintf( fp, "[]" );
	}
}
