// botoc_sqs.h: SQS communication wrapper
// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

// usage:
//  1: include python (first!)
//  2: include this header
//  3: call botoc::set_iam_user( key, secret ) and botoc::set_region( region )
//  4: use as required:
//       botoc::sqs::put( queue, message )
//       botoc::sqs::get( queue, message[, lock[, wait]] )
//       botoc::sqs::delete( queue, handle )
//       botoc::sqs::disconnect( )
//  5: link with python

// changing iam user or region after performing an action has no effect; calling
// disconnect() will close the current connection, and the next request will use
// the new credentials / url

#ifndef BOTOC_SQS_H_INCLUDED__
#define BOTOC_SQS_H_INCLUDED__

#include "botoc_common.h"

/* Earlier versions of boto don't have wait_time_seconds.
 * you can set this in the aws console, but if you want to change it per-request
 * make sure your version of boto is up-to-date and set this to 1 */
#define BOTO_SUPPORTS_WAIT_TIME_SECONDS 0

namespace botoc {
	namespace sqs {
		/* prototypes */
		
		__attribute__((warn_unused_result,unused))
		static bool put( const const_string_t &queue, const const_string_t &message ) _noexcept;
		
		__attribute__((warn_unused_result,unused))
		static handle_t get( const const_string_t &queue, string_t &body, int lockSeconds = 30, int waitSeconds = 0 ) _noexcept;
		
		__attribute__((warn_unused_result,unused))
		static bool remove( const const_string_t &queue, handle_t handle ) _noexcept;
		
		__attribute__((always_inline,unused))
		static inline void disconnect( void ) _noexcept;
		
		/* internal prototypes */
		
		__attribute__((warn_unused_result))
		static PyObject *prep( const const_string_t &queue_name, bool disconnect = false ) _noexcept;
		
		/* implementation */
		
		static PyObject *prep( const const_string_t &queue_name, const bool disconnect ) _noexcept {
			/*
			 * import boto.regioninfo
			 * import boto.sqs.connection
			 *
			 * connection = boto.sqs.connection.SQSConnection(
			 *   aws_access_key_id = [key],
			 *   aws_secret_access_key = [secret],
			 *   region = boto.regioninfo.RegionInfo(
			 *     name = [region],
			 *     endpoint = [region] + '.queue.amazonaws.com'
			 *   )
			 * )
			 *
			 * queue = conn.get_queue( [queue_name] )
			 */
			
			static bool tried = false;
			static PyObject *connection = NULL;
			static std::map<string_t,PyObject*> map;
			
			if( disconnect ) {
				if( connection == NULL ) {
					return NULL;
				}
				
				py_release( connection );
				connection = NULL;
				for( std::map<string_t,PyObject*>::iterator i = map.begin( ); i != map.end( ); ++ i ) {
					py_release( i->second );
				}
				map.clear( );
				tried = false;
				
				return NULL;
			}
			
			if( !tried ) {
				if( unlikely( region.size( ) <= 0 ) ) {
					fprintf( stderr, "attempted to connect to SQS without a region\n" );
					return NULL;
				}
				if( unlikely( user_key.size( ) <= 0 || user_secret.size( ) <= 0 ) ) {
					fprintf( stderr, "attempted to connect to SQS without a valid IAM user\n" );
					return NULL;
				}
				tried = true;
				
				if( unlikely( !py_init( ) ) ) {
					return NULL;
				}
				
				PyObject *regioninfo_mod = py_import( "boto.regioninfo" );
				PyObject *sqs_mod = py_import( "boto.sqs.connection" );
				
				string_t endpoint;
				try {
					endpoint.assign( region );
					endpoint.append( ".queue.amazonaws.com" );
				} catch( ... ) {
					py_release( regioninfo_mod );
					py_release( sqs_mod );
					return NULL;
				}
				connection = py_construct( sqs_mod, "SQSConnection",
					"aws_access_key_id", py_string( user_key ),
					"aws_secret_access_key", py_string( user_secret ),
					"region", py_construct( regioninfo_mod, "RegionInfo",
						"name", py_string( region ),
						"endpoint", py_string( endpoint ),
					NULL ),
				NULL );
				
				py_release( regioninfo_mod );
				py_release( sqs_mod );
				if( unlikely( connection == NULL ) ) {
					fprintf( stderr, "could not connect to SQS\n" );
					return NULL;
				}
			} else if( unlikely( connection == NULL ) ) {
				return NULL;
			}
			
			std::map<string_t,PyObject*>::iterator ind = map.find( queue_name );
			if( ind != map.end( ) ) {
				return ind->second;
			}
			PyObject *queue = py_callfunc( connection, "get_queue",
				"", py_string( queue_name ),
			NULL );
			if( unlikely( queue == NULL ) ) {
				fprintf( stderr, "get_queue failed: %.*s\n", SIZED_STRING(queue_name) );
			}
			if( !PyObject_HasAttrString( queue, "name" ) ) {
				Py_DECREF( queue );
				queue = NULL;
				fprintf( stderr, "queue not found: %.*s\n", SIZED_STRING(queue_name) );
			}
			map.insert( std::pair<string_t,PyObject*>( string_t( queue_name ), queue ) );
			return queue;
		}
		
		static bool put( const const_string_t &queue_name, const const_string_t &message ) _noexcept {
			/*
			 * queue.write( queue.new_message( [message] ) )
			 */
			
			PyObject *queue = prep( queue_name );
			if( unlikely( queue == NULL ) ) {
				return false;
			}
			return py_release_success( py_callfunc( queue, "write",
				"", py_callfunc( queue, "new_message",
					"", py_string( message ),
				NULL ),
			NULL ) );
		}
		static handle_t get( const const_string_t &queue_name, string_t &body, const int lockSeconds, const int waitSeconds ) _noexcept {
			/*
			 * handle = queue.get_messages( visibility_timeout = [lockSeconds], wait_time_seconds = [waitSeconds] )[0]
			 * body = handle.get_body( )
			 */
			
			body.clear( );
			
			PyObject *queue = prep( queue_name );
			if( unlikely( queue == NULL ) ) {
				return NULL;
			}
			PyObject *msg = py_listitem_tmp( py_callfunc( queue, "get_messages",
				(lockSeconds > 0) ? "visibility_timeout" : "-", PyInt_FromLong( (long) lockSeconds ),
#if BOTO_SUPPORTS_WAIT_TIME_SECONDS
				(waitSeconds > 0) ? "wait_time_seconds" : "-", PyInt_FromLong( (long) waitSeconds ),
#endif
			NULL ), 0 );
			
#if !BOTO_SUPPORTS_WAIT_TIME_SECONDS
			(void) waitSeconds;
#endif
			
			PyObject *bod = py_callfunc( msg, "get_body", NULL );
			if( unlikely( bod == NULL ) ) {
				py_release( msg );
				return NULL;
			}
			try {
				body.assign( py_cstring( bod ) );
			} catch( ... ) {
				Py_DECREF( bod );
				Py_DECREF( msg );
				return NULL;
			}
			Py_DECREF( bod );
			
			return (handle_t) msg;
		}
		static bool remove( const const_string_t &queue_name, handle_t handle ) _noexcept {
			/*
			 * handle.delete( )
			 */
			
			(void) queue_name;
			
			if( unlikely( handle == NULL ) ) {
				return false;
			}
			if( unlikely( !Py_IsInitialized( ) ) ) {
				fprintf( stderr, "python has not been initialised\n" );
				return false;
			}
			PyObject *ret = py_callfunc( (PyObject *) handle, "delete", NULL );
			Py_DECREF( (PyObject *) handle );
			return py_release_success( ret );
		}
		static inline void disconnect( void ) _noexcept {
			const const_string_t t;
			(void) prep( t, true );
		}
	}
}

#endif
