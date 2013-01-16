// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

// usage:
//  1: define AWS_REGION, AWS_KEYID, AWS_SECRET constant strings
//  2: include python
//  3: include this header
//  4: call sqs_put / sqs_get / sqs_delete / ddb_update / ddb_get
//  5: link with python

#ifndef AWS_STATIC_INCLUDED__
#define AWS_STATIC_INCLUDED__

#include <vector>
#include <string>
#include <math.h>

#define BASE64_DEFAULT_ALPHABET "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

static inline std::size_t aws_base64( const unsigned char *const string, const std::size_t bytecount, char *const output, const char characters[64] = NULL, bool cap = true, bool term = true ) throw( );
static inline std::size_t aws_unbase64( const unsigned char *const string, const std::size_t length, char *const output, const char characters[64] = NULL, bool term = false ) throw( );


enum DDBType {
	DDB_STRING, // S
	DDB_NUMBER, // N
	DDB_BINARY  // B
};
#define DDBTypeS "SNB" // letters above (in order), used for searching

class ddb_item {
private:
	std::string _name;
	std::size_t _length;
	char *_value;
	DDBType _type;
	
public:
	inline const std::string &name( void ) const throw( ) { return _name; }
	inline std::size_t value_length( void ) const throw( ) { return _length; }
	inline const char *value( void ) const throw( ) { return _value; }
	inline std::size_t binary_value_decoded( void **target ) const throw( ) {
		if( _type != DDB_BINARY ) {
			*target = NULL;
			return 0;
		}
		std::size_t l = aws_unbase64( (const unsigned char *) _value, _length, NULL, NULL, false );
		char *r = (char *) malloc( l * sizeof( char ) );
		if( r != NULL ) {
			l = aws_unbase64( (const unsigned char *) _value, _length, r, NULL, false );
		} else {
			fprintf( stderr, "ddb_item binary_value_decoded: no memory\n" );
		}
		*target = (void *) r;
		return l;
	}
	/*
	inline float float_value( void ) const throw( ) {
		if( _type != DDB_NUMBER ) {
			return 0.0f / 0.0f;
		}
		float r;
		if( snscanf( _value, _length, "%f", &r ) == 1 ) {
			return r;
		} else {
			return 0.0f / 0.0f;
		}
	}
	inline double double_value( void ) const throw( ) {
		if( _type != DDB_NUMBER ) {
			return 0.0 / 0.0;
		}
		double r;
		if( snscanf( _value, _length, "%lf", &r ) == 1 ) {
			return r;
		} else {
			return 0.0 / 0.0;
		}
	}
	inline int int_value( void ) const throw( ) {
		if( _type != DDB_NUMBER ) {
			return 0;
		}
		int r;
		if( snscanf( _value, _length, "%d", &r ) == 1 ) {
			return r;
		} else {
			return 0;
		}
	}
	inline long long_value( void ) const throw( ) {
		if( _type != DDB_NUMBER ) {
			return 0l;
		}
		long r;
		if( snscanf( _value, _length, "%ld", &r ) == 1 ) {
			return r;
		} else {
			return 0l;
		}
	}
	*/
	inline DDBType type( void ) const throw( ) { return _type; }
	
	inline void set_name( const std::string &name ) throw( ) {
		_name = name;
	}
	
	inline void clear( void ) throw( ) {
		_name = "";
		_length = 0;
		if( _value != NULL ) {
			free( _value );
			_value = NULL;
		}
		_type = DDB_STRING;
	}
	
	inline void clear_value( void ) throw( ) {
		_length = 0;
		if( _value != NULL ) {
			free( _value );
			_value = NULL;
		}
	}
	
	inline void set_value_raw( const void *value, std::size_t length, DDBType type ) throw( ) {
		if( length == (std::size_t) -1 ) {
			length = strlen( (char *) value );
		}
		
		if( length > _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( length * sizeof( char ) );
		}
		_length = length;
		if( _value != NULL ) {
			memcpy( _value, value, _length );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value_raw: no memory\n" );
		}
		_type = type;
	}
	
	inline void set_value( const std::string &value ) throw( ) {
		const std::size_t l = value.size( );
		if( l > _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( l * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			memcpy( _value, value.c_str( ), _length );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (std::string): no memory\n" );
		}
	}
	
	inline void set_value( const char *value ) throw( ) {
		const std::size_t l = strlen( value );
		if( l > _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( l * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			memcpy( _value, value, _length );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (std::string): no memory\n" );
		}
	}
	
	inline void set_value( const void *value, std::size_t length, DDBType type ) throw( ) {
		switch( type ) {
			case DDB_STRING:
			case DDB_NUMBER: {
				if( length > _length ) {
					if( _value != NULL ) {
						free( _value );
					}
					_value = (char *) malloc( length * sizeof( char ) );
				}
				_length = length;
				if( _value != NULL ) {
					memcpy( _value, value, _length );
				} else if( _length > 0 ) {
					_length = 0;
					fprintf( stderr, "ddb_item set_value (void *): no memory\n" );
				}
				break;
			}
			case DDB_BINARY: {
				const std::size_t l = aws_base64( (const unsigned char *) value, length, NULL, NULL, true, false );
				if( l > _length ) {
					if( _value != NULL ) {
						free( _value );
					}
					_value = (char *) malloc( l * sizeof( char ) );
				}
				_length = l;
				if( _value != NULL ) {
					_length = aws_base64( (const unsigned char *) value, length, _value, NULL, true, false );
				} else {
					_length = 0;
					fprintf( stderr, "ddb_item set_value (void *): no memory\n" );
				}
				break;
			}
			default: {
				fprintf( stderr, "ddb_item set_value (void *): unknown data type\n" );
				_length = 0;
				if( _value != NULL ) {
					free( _value );
					_value = NULL;
				}
				_type = DDB_STRING;
				break;
			}
		}
	}
	inline void set_value( const void *value, std::size_t length ) throw( ) {
		set_value( value, length, _type );
	}
	
	inline void set_value( float value ) throw( ) {
		const std::size_t l = (std::size_t) snprintf( NULL, 0, "%f", value );
		if( l >= _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( (l + 1) * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%f", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (float): no memory\n" );
		}
	}
	
	inline void set_value( double value ) throw( ) {
		const std::size_t l = (std::size_t) snprintf( NULL, 0, "%f", value );
		if( l >= _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( (l + 1) * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%f", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (double): no memory\n" );
		}
	}
	
	inline void set_value( int value ) throw( ) {
		const std::size_t l = (std::size_t) snprintf( NULL, 0, "%d", value );
		if( l >= _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( (l + 1) * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%d", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (int): no memory\n" );
		}
	}
	
	inline void set_value( long value ) throw( ) {
		const std::size_t l = (std::size_t) snprintf( NULL, 0, "%ld", value );
		if( l >= _length ) {
			if( _value != NULL ) {
				free( _value );
			}
			_value = (char *) malloc( (l + 1) * sizeof( char ) );
		}
		_length = l;
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%ld", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item set_value (long): no memory\n" );
		}
	}
	
	inline ddb_item( void ) throw( ) :
	_name( ),
	_length( 0 ),
	_value( NULL ),
	_type( DDB_STRING )
	{
	}
	
	inline explicit ddb_item( const std::string &name ) throw( ) :
	_name( name ),
	_length( 0 ),
	_value( NULL ),
	_type( DDB_STRING )
	{
	}
	
	inline ddb_item( const std::string &name, const std::string &value ) throw( ) :
	_name( name ),
	_length( value.size( ) ), // does NOT include \0
	_value( (char *) malloc( _length * sizeof( char ) ) ),
	_type( DDB_STRING )
	{
		if( _value != NULL ) {
			memcpy( _value, value.c_str( ), _length );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (std::string): no memory\n" );
		}
	}
	
	inline ddb_item( const std::string &name, const char *value ) throw( ) :
	_name( name ),
	_length( strlen( value ) ), // does NOT include \0
	_value( (char *) malloc( _length * sizeof( char ) ) ),
	_type( DDB_STRING )
	{
		if( _value != NULL ) {
			memcpy( _value, value, _length );
		} else {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (char *): no memory\n" );
		}
	}
	
	inline ddb_item( const std::string &name, const void *value, std::size_t length, DDBType type = DDB_STRING ) throw( ) :
	_name( name ),
	_length( 0 ),
	_value( NULL ),
	_type( type )
	{
		switch( type ) {
			case DDB_STRING:
			case DDB_NUMBER:
				_length = length;
				_value = (char *) malloc( _length * sizeof( char ) );
				if( _value != NULL ) {
					memcpy( _value, value, _length );
				} else if( _length > 0 ) {
					_length = 0;
					fprintf( stderr, "ddb_item constructor (void *): no memory\n" );
				}
				break;
			case DDB_BINARY:
				_length = aws_base64( (const unsigned char *) value, length, NULL, NULL, true, false );
				_value = (char *) malloc( _length * sizeof( char ) );
				if( _value != NULL ) {
					_length = aws_base64( (const unsigned char *) value, length, _value, NULL, true, false );
				} else if( _length > 0 ) {
					_length = 0;
					fprintf( stderr, "ddb_item constructor (void *): no memory\n" );
				}
				break;
			default:
				fprintf( stderr, "ddb_item constructor (void *): unknown data type\n" );
				_type = DDB_STRING;
				break;
		}
	}
	
	inline ddb_item( const std::string &name, const void *value, std::size_t length, DDBType type, bool raw ) throw( ) :
	_name( name ),
	_length( 0 ),
	_value( NULL ),
	_type( type )
	{
		if( !raw ) {
			switch( type ) {
				case DDB_STRING:
				case DDB_NUMBER:
					_length = length;
					_value = (char *) malloc( _length * sizeof( char ) );
					if( _value != NULL ) {
						memcpy( _value, value, _length );
					} else if( _length > 0 ) {
						_length = 0;
						fprintf( stderr, "ddb_item constructor (void *): no memory\n" );
					}
					break;
				case DDB_BINARY:
					_length = aws_base64( (const unsigned char *) value, length, NULL, NULL, true, false );
					_value = (char *) malloc( _length * sizeof( char ) );
					if( _value != NULL ) {
						_length = aws_base64( (const unsigned char *) value, length, _value, NULL, true, false );
					} else if( _length > 0 ) {
						_length = 0;
						fprintf( stderr, "ddb_item constructor (void *): no memory\n" );
					}
					break;
				default:
					fprintf( stderr, "ddb_item constructor (void *): unknown data type\n" );
					_type = DDB_STRING;
					break;
			}
		} else {
			if( length == (std::size_t) -1 ) {
				length = strlen( (const char *) value );
			}
			_length = length;
			_value = (char *) malloc( _length * sizeof( char ) );
			if( _value != NULL ) {
				memcpy( _value, value, _length );
			} else if( _length > 0 ) {
				_length = 0;
				fprintf( stderr, "ddb_item constructor (raw): no memory\n" );
			}
		}
	}
	
	inline ddb_item( const std::string &name, float value ) throw( ) :
	_name( name ),
	_length( snprintf( NULL, 0, "%f", value ) ),
	_value( (char *) malloc( (_length + 1) * sizeof( char ) ) ),
	_type( DDB_NUMBER )
	{
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%f", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (float): no memory\n" );
		}
	}
	inline ddb_item( const std::string &name, double value ) throw( ) :
	_name( name ),
	_length( snprintf( NULL, 0, "%f", value ) ),
	_value( (char *) malloc( (_length + 1) * sizeof( char ) ) ),
	_type( DDB_NUMBER )
	{
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%f", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (double): no memory\n" );
		}
	}
	inline ddb_item( const std::string &name, int value ) throw( ) :
	_name( name ),
	_length( snprintf( NULL, 0, "%d", value ) ),
	_value( (char *) malloc( (_length + 1) * sizeof( char ) ) ),
	_type( DDB_NUMBER )
	{
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%d", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (int): no memory\n" );
		}
	}
	inline ddb_item( const std::string &name, long value ) throw( ) :
	_name( name ),
	_length( snprintf( NULL, 0, "%ld", value ) ),
	_value( (char *) malloc( (_length + 1) * sizeof( char ) ) ),
	_type( DDB_NUMBER )
	{
		if( _value != NULL ) {
			(void) snprintf( _value, _length + 1, "%ld", value );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item constructor (long): no memory\n" );
		}
	}
	
	inline ddb_item( const ddb_item &copy ) throw( ) :
	_name( copy._name ),
	_length( copy._length ),
	_value( (char *) malloc( copy._length * sizeof( char ) ) ),
	_type( copy._type )
	{
		if( _value != NULL ) {
			memcpy( _value, copy._value, _length );
		} else if( _length > 0 ) {
			_length = 0;
			fprintf( stderr, "ddb_item copy constructor: no memory\n" );
		}
	}
	
	inline ~ddb_item( void ) throw( ) {
		if( _value != NULL ) {
			free( _value );
		}
	}
};

// internal
static PyObject *ddb_prep( void ) throw( );
static PyObject *PyDict_from_ddbitems( const std::vector<ddb_item> &items );
static PyObject *PyList_from_ddbitems( const std::vector<ddb_item> &items );
static void update_ddbitems_from_PyDict( std::vector<ddb_item> &items, PyObject *ret_items );

// external
static bool ddb_update( const std::string &db, const std::string &key, const std::vector<ddb_item> &items, const std::vector<ddb_item> *expected = NULL ) throw( );
static bool ddb_get( const std::string &db, const std::string &key, bool consistent, std::vector<ddb_item> &items ) throw( );

static bool sqs_put( const std::string &queue, const std::string &message ) throw( );
static bool sqs_get( const std::string &queue, void **handle, std::string &body, int lockSeconds = 30, int waitSeconds = 0 ) throw( );
static bool sqs_delete( const std::string &queue, void **handle ) throw( );







// implementation
static PyObject *ddb_prep( void ) throw( ) {
	static bool tried = false;
	static PyObject *layer1 = NULL;
	if( tried ) {
		return layer1;
	}
	tried = true;
	
	Py_Initialize( );
	
	// import boto.regioninfo
	// import boto.dynamodb.layer1
	// reg = boto.regioninfo.RegionInfo( name = AWS_REGION, endpoint = 'dynamodb.' AWS_REGION '.amazonaws.com' )
	// layer1 = boto.dynamodb.layer1.Layer1( aws_access_key_id = [key], aws_secret_access_key = [secret], region = reg )
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "initializing python threw exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		return NULL;
	}
	
	PyObject *boto_regioninfo_str = PyString_FromString( "boto.regioninfo" );
	PyObject *boto_regioninfo_mod = PyImport_Import( boto_regioninfo_str );
	Py_DECREF( boto_regioninfo_str );
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "loading boto.regioninfo threw exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( boto_regioninfo_mod != NULL ) {
			Py_DECREF( boto_regioninfo_mod );
		}
		return NULL;
	}
	if( boto_regioninfo_mod == NULL ) {
		fprintf( stderr, "boto not installed, or no boto.regioninfo\n" );
		return NULL;
	}
	PyObject *boto_dynamodb_layer1_str = PyString_FromString( "boto.dynamodb.layer1" );
	PyObject *boto_dynamodb_layer1_mod = PyImport_Import( boto_dynamodb_layer1_str );
	Py_DECREF( boto_dynamodb_layer1_str );
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "loading boto.dynamodb.layer1 threw exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( boto_regioninfo_mod );
		if( boto_dynamodb_layer1_mod != NULL ) {
			Py_DECREF( boto_dynamodb_layer1_mod );
		}
		return NULL;
	}
	if( boto_dynamodb_layer1_mod == NULL ) {
		fprintf( stderr, "no boto.dynamodb.layer1\n" );
		Py_DECREF( boto_regioninfo_mod );
		return NULL;
	}
	PyObject *regioninfo_ns = PyModule_GetDict( boto_regioninfo_mod ); // borrowed
	if( regioninfo_ns == NULL ) {
		fprintf( stderr, "boto.regioninfo has no dictionary\n" );
		Py_DECREF( boto_regioninfo_mod );
		Py_DECREF( boto_dynamodb_layer1_mod );
		return NULL;
	}
	PyObject *layer1_ns = PyModule_GetDict( boto_dynamodb_layer1_mod ); // borrowed
	if( layer1_ns == NULL ) {
		fprintf( stderr, "boto.dynamodb.layer1 has no dictionary\n" );
		Py_DECREF( boto_regioninfo_mod );
		Py_DECREF( boto_dynamodb_layer1_mod );
		return NULL;
	}
	
	PyObject *regioninfo_cls = PyDict_GetItemString( regioninfo_ns, "RegionInfo" );
	Py_DECREF( boto_regioninfo_mod );
	if( regioninfo_cls == NULL ) {
		fprintf( stderr, "boto does not contain the RegionInfo object\n" );
		Py_DECREF( boto_dynamodb_layer1_mod );
		return NULL;
	}
	
	PyObject *layer1_cls = PyDict_GetItemString( layer1_ns, "Layer1" );
	Py_DECREF( boto_dynamodb_layer1_mod );
	if( layer1_cls == NULL ) {
		fprintf( stderr, "boto does not contain the Layer1 dynamodb object\n" );
		Py_DECREF( regioninfo_cls );
		return NULL;
	}
	
	PyObject *reg;
	
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		PyObject *arg_dict = PyDict_New( );
		
		PyObject *arg_dict_name = PyString_FromString( AWS_REGION );
		PyDict_SetItemString( arg_dict, "name", arg_dict_name );
		Py_DECREF( arg_dict_name );
		PyObject *arg_dict_endpoint = PyString_FromString( "dynamodb." AWS_REGION ".amazonaws.com" );
		PyDict_SetItemString( arg_dict, "endpoint", arg_dict_endpoint );
		Py_DECREF( arg_dict_endpoint );
		
		reg = PyObject_Call( regioninfo_cls, arg_list, arg_dict );
		
		Py_DECREF( arg_list );
		Py_DECREF( arg_dict );
	}
	
	Py_DECREF( regioninfo_cls );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "creating region threw exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( layer1_cls );
		if( reg != NULL ) {
			Py_DECREF( reg );
		}
		return NULL;
	}
	if( reg == NULL ) {
		fprintf( stderr, "creating region failed\n" );
		Py_DECREF( layer1_cls );
		return NULL;
	}
	
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		PyObject *arg_dict = PyDict_New( );
		
		PyObject *arg_dict_key = PyString_FromString( AWS_KEYID );
		PyDict_SetItemString( arg_dict, "aws_access_key_id", arg_dict_key );
		Py_DECREF( arg_dict_key );
		
		PyObject *arg_dict_sec = PyString_FromString( AWS_SECRET );
		PyDict_SetItemString( arg_dict, "aws_secret_access_key", arg_dict_sec );
		Py_DECREF( arg_dict_sec );
		
		PyDict_SetItemString( arg_dict, "region", reg );
		Py_DECREF( reg );
		
		layer1 = PyObject_Call( layer1_cls, arg_list, arg_dict );
		
		Py_DECREF( arg_list );
		Py_DECREF( arg_dict );
	}
	
	Py_DECREF( layer1_cls );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "creating dynamodb.layer1.Layer1 threw exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( layer1 != NULL ) {
			Py_DECREF( layer1 );
			layer1 = NULL;
		}
		return NULL;
	}
	if( layer1 == NULL ) {
		fprintf( stderr, "creating dynamodb.layer1.Layer1 failed\n" );
		return NULL;
	}
	
	return layer1;
}

PyObject *PyDict_from_ddbitems( const std::vector<ddb_item> &items ) {
	if( !Py_IsInitialized( ) ) {
		fprintf( stderr, "python has not been initialised\n" );
		return NULL;
	}
	PyObject *r = PyDict_New( );
	for( std::size_t i = 0, e = items.size( ); i < e; ++ i ) {
		if( items[i].value_length( ) == 0 ) {
			continue; // AWS complains if a value is blank and refuses to do anything
		}
		PyObject *t = PyDict_New( );
		PyObject *o = PyDict_New( );
		char m[2] = { DDBTypeS[items[i].type()], '\0' };
		PyObject *s = PyString_FromStringAndSize( items[i].value( ), items[i].value_length( ) );
		PyDict_SetItemString( o, m, s );
		Py_DECREF( s );
		PyDict_SetItemString( t, "Value", o );
		Py_DECREF( o );
		PyObject *key = PyString_FromString( items[i].name( ).c_str( ) );
		PyDict_SetItem( r, key, t );
		Py_DECREF( key );
		Py_DECREF( t );
	}
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "PyDict_from_ddbitems hit exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( r );
		return NULL;
	}
	return r;
}
PyObject *PyList_from_ddbitems( const std::vector<ddb_item> &items ) {
	if( !Py_IsInitialized( ) ) {
		fprintf( stderr, "python has not been initialised\n" );
		return NULL;
	}
	const std::size_t e = items.size( );
	PyObject *r = PyList_New( e );
	for( std::size_t i = 0; i < e; ++ i ) {
		PyObject *key = PyString_FromString( items[i].name( ).c_str( ) );
		PyList_SET_ITEM( r, i, key );
		//		Py_DECREF( key );
	}
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "PyList_from_ddbitems hit exception: %s (%s)\n", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( r );
		return NULL;
	}
	return r;
}
void update_ddbitems_from_PyDict( std::vector<ddb_item> &items, PyObject *ret_items ) {
	if( items.size( ) == 0 ) {
		PyObject *key; // borrowed
		PyObject *itm; // borrowed
		Py_ssize_t i = 0;
		while( PyDict_Next( ret_items, &i, &key, &itm ) ) {
			// exactly 1 attribute, but with unknown key, so just fetch an attribute at random
			PyObject *type; // borrowed
			PyObject *value; // borrowed
			Py_ssize_t pos = 0;
			if( !PyDict_Next( itm, &pos, &type, &value ) ) {
				fprintf( stderr, "malformed record (no data) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			const char *t = PyString_AsString( type );
			const char *v = PyString_AsString( value );
			if( t == NULL ) {
				fprintf( stderr, "malformed record (type is not a string) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			if( v == NULL ) {
				fprintf( stderr, "malformed record (value is not a string) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			const char *const t2 = strchr( DDBTypeS, t[0] );
			if( t2 == NULL ) {
				fprintf( stderr, "malformed record (no type) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			const ddb_item itm( PyString_AsString( key ), v, -1, (DDBType) ((std::size_t) (t2 - DDBTypeS) / sizeof( char )), true );
			items.push_back( itm );
		}
	} else {
		for( std::size_t i = items.size( ); (i --) > 0; ) {
			PyObject *key = PyString_FromString( items[i].name( ).c_str( ) );
			PyObject *itm = PyDict_GetItem( ret_items, key ); // borrowed
			Py_DECREF( key );
			
			if( itm == NULL ) {
				items.erase( items.begin( ) + i );
				continue;
			}
			
			// exactly 1 attribute, but with unknown key, so just fetch an attribute at random
			PyObject *type; // borrowed
			PyObject *value; // borrowed
			Py_ssize_t pos = 0;
			if( !PyDict_Next( itm, &pos, &type, &value ) ) {
				fprintf( stderr, "malformed record (no data) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			const char *t = PyString_AsString( type );
			const char *v = PyString_AsString( value );
			if( t == NULL ) {
				fprintf( stderr, "malformed record (type is not a string) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			if( v == NULL ) {
				fprintf( stderr, "malformed record (value is not a string) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			const char *const t2 = strchr( DDBTypeS, t[0] );
			if( t2 == NULL ) {
				fprintf( stderr, "malformed record (no type) for key %s\n", items[i].name( ).c_str( ) );
				continue;
			}
			items[i].set_value_raw( v, -1, (DDBType) ((std::size_t) (t2 - DDBTypeS) / sizeof( char )) );
		}
	}
}

static bool ddb_update( const std::string &db, const std::string &key, const std::vector<ddb_item> &items, const std::vector<ddb_item> *expected ) throw( ) {
	// http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_UpdateItem.html
	
	PyObject *layer1 = ddb_prep( );
	if( layer1 == NULL ) {
		return false;
	}
	
	// layer1.update_item( [table], {'HashKeyElement':{'S':[key]}}, {[attr1]:{'Value':{[T]:[value]}}} )
	
	PyObject *update_item_f = PyObject_GetAttrString( layer1, "update_item" );
	if( update_item_f == NULL ) {
		fprintf( stderr, "boto dynamodb does not have update_item\n" );
		return false;
	}
	
	PyObject *arg_list = PyTuple_New( 3 );
	PyTuple_SET_ITEM( arg_list, 0, PyString_FromString( db.c_str( ) ) );
	PyObject *key_dict = PyDict_New( );
	PyObject *key_str = PyString_FromString( key.c_str( ) );
	PyObject *key_prop = PyDict_New( );
	PyDict_SetItemString( key_prop, "S", key_str );
	Py_DECREF( key_str );
	PyDict_SetItemString( key_dict, "HashKeyElement", key_prop );
	Py_DECREF( key_prop );
	PyTuple_SET_ITEM( arg_list, 1, key_dict );
	PyTuple_SET_ITEM( arg_list, 2, PyDict_from_ddbitems( items ) );
	
	PyObject *arg_dict = PyDict_New( );
	if( expected != NULL ) {
		if( expected->size( ) > 0 ) {
			PyObject *expect = PyDict_from_ddbitems( *expected );
			PyDict_SetItemString( arg_dict, "expected", expect );
			Py_DECREF( expect );
		}
	}
	
	PyObject *ret;
	for( int attempt = 0; attempt < 3; ++ attempt ) {
		if( attempt > 0 ) {
			sleep( (int) (1.0f * powf( 2.0f, (float) attempt - 1.0f )) );
			// provisioning is per second, so try waiting until the next second
			// waits 1 second, 2 seconds, 4 seconds, ...
			fprintf( stderr, "ddb_update attempt %d", attempt + 1 );
		}
		ret = PyObject_Call( update_item_f, arg_list, arg_dict );
		
		if( PyErr_Occurred( ) != NULL ) {
			PyObject *ex_type;
			PyObject *ex_value;
			PyObject *ex_traceback;
			PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
			PyObject *ex_type_name = PyObject_Str( ex_type );
			PyObject *ex_value_str = PyObject_Str( ex_value );
			if( ex_value != NULL ) { Py_DECREF( ex_value ); }
			if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
			Py_DECREF( ex_type );
			if( ret != NULL ) {
				Py_DECREF( ret );
			}
			const char *m = PyString_AsString( ex_type_name );
			if( strcmp( m, "<class 'boto.dynamodb.exceptions.ProvisionedThroughputExceededException'>" ) == 0 ) {
				fprintf( stderr, "throughput exceeded when saving to table \"%s\": %s\n", db.c_str( ), m );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				continue;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.ThrottlingException'>" ) == 0 ) {
				fprintf( stderr, "throttled when saving to table \"%s\": %s\n", db.c_str( ), m );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				continue;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.DynamoDBConditionalCheckFailedError'>" ) == 0 ) {
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( update_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.ResourceNotFoundException'>" ) == 0 ) {
				fprintf( stderr, "table \"%s\" not found for updating: %s\n", db.c_str( ), m );
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( update_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			} else {
				fprintf( stderr, "update_item threw unrecognised exception: %s (%s)\n", m, PyString_AsString( ex_value_str ) );
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( update_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			}
		} else if( ret == NULL ) {
			fprintf( stderr, "failed to save record in table \"%s\"\n", db.c_str( ) );
			Py_DECREF( arg_list );
			Py_DECREF( arg_dict );
			Py_DECREF( update_item_f );
			return false;
		} else {
			break;
		}
	}
	Py_DECREF( arg_list );
	Py_DECREF( arg_dict );
	Py_DECREF( update_item_f );
	
	PyObject *cap = PyDict_GetItemString( ret, "ConsumedCapacityUnits" ); // borrowed
	if( cap == NULL ) {
		fprintf( stderr, "bad response when saving record in table \"%s\"\n", db.c_str( ) );
		Py_DECREF( ret );
		return false;
	}
	fprintf( stderr, "saved record, used %f capacity units\n", PyFloat_AsDouble( cap ) );
	
	Py_DECREF( ret );
	
	return true;
}
static bool ddb_get( const std::string &db, const std::string &key, bool consistent, std::vector<ddb_item> &items ) throw( ) {
	// http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_GetItem.html
	
	PyObject *layer1 = ddb_prep( );
	if( layer1 == NULL ) {
		return false;
	}
	
	// get_item( [database_name], {'HashKeyElement':{'S':[key]}} )
	
	PyObject *get_item_f = PyObject_GetAttrString( layer1, "get_item" );
	
	if( get_item_f == NULL ) {
		fprintf( stderr, "boto dynamodb does not have get_item\n" );
		return false;
	}
	
	PyObject *arg_list = PyTuple_New( 2 );
	PyTuple_SET_ITEM( arg_list, 0, PyString_FromString( db.c_str( ) ) );
	PyObject *key_dict = PyDict_New( );
	PyObject *key_str = PyString_FromString( key.c_str( ) );
	PyObject *key_prop = PyDict_New( );
	PyDict_SetItemString( key_prop, "S", key_str );
	Py_DECREF( key_str );
	PyDict_SetItemString( key_dict, "HashKeyElement", key_prop );
	Py_DECREF( key_prop );
	PyTuple_SET_ITEM( arg_list, 1, key_dict );
	
	PyObject *arg_dict = PyDict_New( );
	PyObject *gets = PyList_from_ddbitems( items );
	PyDict_SetItemString( arg_dict, "attributes_to_get", gets );
	Py_DECREF( gets );
	//	PyObject *yes = PyBool_FromLong( 1 );
	PyDict_SetItemString( arg_dict, "consistent_read", consistent ? Py_True : Py_False );
	//	Py_DECREF( yes );
	
	PyObject *ret;
	for( int attempt = 0; attempt < 3; ++ attempt ) {
		if( attempt > 0 ) {
			sleep( (int) (1.0f * powf( 2.0f, (float) attempt - 1.0f )) );
			// provisioning is per second, so try waiting until the next second
			// waits 1 second, 2 seconds, 4 seconds, ...
			fprintf( stderr, "ddb_get attempt %d", attempt + 1 );
		}
		ret = PyObject_Call( get_item_f, arg_list, arg_dict );
		
		if( PyErr_Occurred( ) != NULL ) {
			PyObject *ex_type;
			PyObject *ex_value;
			PyObject *ex_traceback;
			PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
			PyObject *ex_type_name = PyObject_Str( ex_type );
			PyObject *ex_value_str = PyObject_Str( ex_value );
			if( ex_value != NULL ) { Py_DECREF( ex_value ); }
			if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
			Py_DECREF( ex_type );
			if( ret != NULL ) {
				Py_DECREF( ret );
			}
			const char *m = PyString_AsString( ex_type_name );
			if( strcmp( m, "<class 'boto.dynamodb.exceptions.ProvisionedThroughputExceededException'>" ) == 0 ) {
				fprintf( stderr, "throughput exceeded when reading table \"%s\": %s", db.c_str( ), m );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				continue;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.DynamoDBKeyNotFoundError'>" ) == 0 ) {
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( get_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.ThrottlingException'>" ) == 0 ) {
				fprintf( stderr, "throttled when reading table \"%s\": %s", db.c_str( ), m );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				continue;
			} else if( strcmp( m, "<class 'boto.dynamodb.exceptions.ResourceNotFoundException'>" ) == 0 ) {
				fprintf( stderr, "table \"%s\" not found for reading: %s", db.c_str( ), m );
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( get_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			} else {
				fprintf( stderr, "update_item threw unrecognised exception: %s (%s)", m, PyString_AsString( ex_value_str ) );
				Py_DECREF( arg_list );
				Py_DECREF( arg_dict );
				Py_DECREF( get_item_f );
				Py_DECREF( ex_type_name );
				Py_DECREF( ex_value_str );
				return false;
			}
		} else if( ret == NULL ) {
			fprintf( stderr, "failed to load record from table \"%s\"", db.c_str( ) );
			Py_DECREF( arg_list );
			Py_DECREF( arg_dict );
			Py_DECREF( get_item_f );
			return false;
		} else {
			break;
		}
	}
	Py_DECREF( arg_list );
	Py_DECREF( arg_dict );
	Py_DECREF( get_item_f );
	
	PyObject *cap = PyDict_GetItemString( ret, "ConsumedCapacityUnits" ); // borrowed
	PyObject *ret_items = PyDict_GetItemString( ret, "Item" ); // borrowed
	if( ret_items == NULL ) {
		fprintf( stderr, "failed to load record items from table \"%s\"", db.c_str( ) );
		Py_DECREF( ret );
		return false;
	}
	if( cap == NULL ) {
		fprintf( stderr, "loaded record, but capacity units used is unknown" );
	} else {
		fprintf( stderr, "loaded record, used %f capacity units", PyFloat_AsDouble( cap ) );
	}
	
	update_ddbitems_from_PyDict( items, ret_items );
	
	Py_DECREF( ret );
	
	return true;
}

static PyObject *sqs_prep( const std::string &queue_name ) throw( );

static PyObject *sqs_prep( const std::string &queue_name ) throw( ) {
	static bool tried = false;
	static PyObject *queue = NULL;
	if( tried ) {
		return queue;
	}
	tried = true;
	
	Py_Initialize( );
	
	// import boto.regioninfo
	// import boto.sqs.connection
	// reg = boto.regioninfo.RegionInfo( name = AWS_REGION, endpoint = 'dynamodb.' AWS_REGION '.amazonaws.com' )
	// conn = boto.sqs.connection.SQSConnection( aws_access_key_id = [key], aws_secret_access_key = [secret], region = reg )
	// queue = conn.get_queue( [queue_name] )
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "initializing python threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		return NULL;
	}
	
	PyObject *boto_regioninfo_str = PyString_FromString( "boto.regioninfo" );
	PyObject *boto_regioninfo_mod = PyImport_Import( boto_regioninfo_str );
	Py_DECREF( boto_regioninfo_str );
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "loading boto.regioninfo threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( boto_regioninfo_mod != NULL ) {
			Py_DECREF( boto_regioninfo_mod );
		}
		return NULL;
	}
	if( boto_regioninfo_mod == NULL ) {
		fprintf( stderr, "boto not installed, or no boto.regioninfo\n" );
		return NULL;
	}
	PyObject *boto_sqs_connection_str = PyString_FromString( "boto.sqs.connection" );
	PyObject *boto_sqs_connection_mod = PyImport_Import( boto_sqs_connection_str );
	Py_DECREF( boto_sqs_connection_str );
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "loading boto.sqs.connection threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( boto_regioninfo_mod );
		if( boto_sqs_connection_mod != NULL ) {
			Py_DECREF( boto_sqs_connection_mod );
		}
		return NULL;
	}
	if( boto_sqs_connection_mod == NULL ) {
		fprintf( stderr, "no boto.sqs.connection\n" );
		Py_DECREF( boto_regioninfo_mod );
		return NULL;
	}
	PyObject *regioninfo_ns = PyModule_GetDict( boto_regioninfo_mod ); // borrowed
	if( regioninfo_ns == NULL ) {
		fprintf( stderr, "boto.regioninfo has no dictionary\n" );
		Py_DECREF( boto_regioninfo_mod );
		Py_DECREF( boto_sqs_connection_mod );
		return NULL;
	}
	PyObject *sqs_ns = PyModule_GetDict( boto_sqs_connection_mod ); // borrowed
	if( sqs_ns == NULL ) {
		fprintf( stderr, "boto.sqs.connection has no dictionary\n" );
		Py_DECREF( boto_regioninfo_mod );
		Py_DECREF( boto_sqs_connection_mod );
		return NULL;
	}
	
	PyObject *regioninfo_cls = PyDict_GetItemString( regioninfo_ns, "RegionInfo" );
	Py_DECREF( boto_regioninfo_mod );
	if( regioninfo_cls == NULL ) {
		fprintf( stderr, "boto does not contain the RegionInfo object\n" );
		Py_DECREF( boto_sqs_connection_mod );
		return NULL;
	}
	
	PyObject *sqsconnection_cls = PyDict_GetItemString( sqs_ns, "SQSConnection" );
	Py_DECREF( boto_sqs_connection_mod );
	if( sqsconnection_cls == NULL ) {
		fprintf( stderr, "boto does not contain the SQSConnection object\n" );
		Py_DECREF( regioninfo_cls );
		return NULL;
	}
	
	PyObject *reg;
	
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		PyObject *arg_dict = PyDict_New( );
		
		PyObject *arg_dict_name = PyString_FromString( AWS_REGION );
		PyDict_SetItemString( arg_dict, "name", arg_dict_name );
		Py_DECREF( arg_dict_name );
		PyObject *arg_dict_endpoint = PyString_FromString( AWS_REGION ".queue.amazonaws.com" );
		PyDict_SetItemString( arg_dict, "endpoint", arg_dict_endpoint );
		Py_DECREF( arg_dict_endpoint );
		
		reg = PyObject_Call( regioninfo_cls, arg_list, arg_dict );
		
		Py_DECREF( arg_list );
		Py_DECREF( arg_dict );
	}
	
	Py_DECREF( regioninfo_cls );
	
	PyObject *sqsconnection;
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		PyObject *arg_dict = PyDict_New( );
		
		PyObject *arg_dict_key = PyString_FromString( AWS_KEYID );
		PyDict_SetItemString( arg_dict, "aws_access_key_id", arg_dict_key );
		Py_DECREF( arg_dict_key );
		
		PyObject *arg_dict_sec = PyString_FromString( AWS_SECRET );
		PyDict_SetItemString( arg_dict, "aws_secret_access_key", arg_dict_sec );
		Py_DECREF( arg_dict_sec );
		
		PyDict_SetItemString( arg_dict, "region", reg );
		Py_DECREF( reg );
		
		sqsconnection = PyObject_Call( sqsconnection_cls, arg_list, arg_dict );
		
		Py_DECREF( arg_list );
		Py_DECREF( arg_dict );
	}
	
	Py_DECREF( sqsconnection_cls );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "creating SQSConnection threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( sqsconnection != NULL ) {
			Py_DECREF( sqsconnection );
		}
		return NULL;
	}
	if( sqsconnection == NULL ) {
		fprintf( stderr, "creating SQSConnection failed\n" );
		return NULL;
	}
	
	PyObject *get_queue_f = PyObject_GetAttrString( sqsconnection, "get_queue" );
	if( get_queue_f == NULL ) {
		fprintf( stderr, "SQSConnection does not have get_queue\n" );
		Py_DECREF( sqsconnection );
		return NULL;
	}
	
	{
		PyObject *arg_list = PyTuple_New( 1 );
		
		PyTuple_SET_ITEM( arg_list, 0, PyString_FromString( queue_name.c_str( ) ) );
		
		queue = PyObject_Call( get_queue_f, arg_list, NULL );
		
		Py_DECREF( arg_list );
	}
	
	Py_DECREF( sqsconnection );
	Py_DECREF( get_queue_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "get_queue threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( queue != NULL ) {
			Py_DECREF( queue );
			queue = NULL;
		}
		return NULL;
	}
	if( queue == NULL ) {
		fprintf( stderr, "get_queue failed\n" );
		return NULL;
	}
	
	return queue;
}

static bool sqs_put( const std::string &queue_name, const std::string &message ) throw( ) {
	// to improve: the first queue is used for all later calls. Some day this could be confusing
	
	PyObject *queue = sqs_prep( queue_name );
	if( queue == NULL ) {
		return false;
	}
	
	// queue.write( queue.new_message( [message] ) )
	
	PyObject *new_message_f = PyObject_GetAttrString( queue, "new_message" );
	if( new_message_f == NULL ) {
		fprintf( stderr, "boto queue does not have new_message\n" );
		return false;
	}
	
	PyObject *write_f = PyObject_GetAttrString( queue, "write" );
	if( write_f == NULL ) {
		fprintf( stderr, "boto queue does not have write\n" );
		Py_DECREF( new_message_f );
		return false;
	}
	
	PyObject *msg;
	
	{
		PyObject *arg_list = PyTuple_New( 1 );
		PyTuple_SET_ITEM( arg_list, 0, PyString_FromString( message.c_str( ) ) );
		
		msg = PyObject_Call( new_message_f, arg_list, NULL );
		
		Py_DECREF( arg_list );
	}
	Py_DECREF( new_message_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "sqs.new_message threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( msg != NULL ) {
			Py_DECREF( msg );
		}
		return false;
	}
	if( msg == NULL ) {
		fprintf( stderr, "sqs.new_message failed\n" );
		Py_DECREF( write_f );
		return false;
	}
	
	PyObject *ret;
	
	{
		PyObject *arg_list = PyTuple_New( 1 );
		PyTuple_SET_ITEM( arg_list, 0, msg );
		
		ret = PyObject_Call( write_f, arg_list, NULL );
		
		Py_DECREF( arg_list );
	}
	Py_DECREF( write_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		Py_DECREF( ex_type );
		if( ret != NULL ) {
			Py_DECREF( ret );
		}
		const char *m = PyString_AsString( ex_type_name );
		/*
		 if( strcmp( m, "<class 'boto.dynamodb.exceptions.ResourceNotFoundException'>" ) == 0 ) {
		 fprintf( stderr, "table \"%s\" not found for updating: %s\n", db.c_str( ), m );
		 Py_DECREF( ex_type_name );
		 Py_DECREF( ex_value_str );
		 return false;
		 } else {*/
		fprintf( stderr, "sqs.write threw unrecognised exception: %s (%s)\n", m, PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		return false;
		//		}
	}
	if( ret == NULL ) {
		fprintf( stderr, "sqs.write failed\n" );
		return false;
	}
	
	Py_DECREF( ret );
	
	return true;
}
static bool sqs_get( const std::string &queue_name, void **handle, std::string &body, int lockSeconds, int waitSeconds ) throw( ) {
	// to improve: the first queue is used for all later calls. Some day this could be confusing
	
	*handle = NULL;
	
	PyObject *queue = sqs_prep( queue_name );
	if( queue == NULL ) {
		return false;
	}
	
	// str = queue.get_messages( visibility_timeout = [lockSeconds], wait_time_seconds = [waitSeconds] )[0].get_body( )
	
	PyObject *get_messages_f = PyObject_GetAttrString( queue, "get_messages" );
	if( get_messages_f == NULL ) {
		fprintf( stderr, "boto queue does not have get_messages\n" );
		return false;
	}
	
	PyObject *msgs;
	
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		PyObject *arg_dict = PyDict_New( );
		
		if( lockSeconds > 0 ) {
			PyObject *arg_dict_lock = PyInt_FromLong( (long) lockSeconds );
			PyDict_SetItemString( arg_dict, "visibility_timeout", arg_dict_lock );
			Py_DECREF( arg_dict_lock );
		}
		// to improve: boto seems to be an earlier version than it claims to be; wait_time_seconds isn't defined
		// nevermind for now; the delay can be set in the console
		(void) waitSeconds;
		/*
		 if( waitSeconds > 0 ) {
		 PyObject *arg_dict_wait = PyInt_FromLong( (long) waitSeconds );
		 PyDict_SetItemString( arg_dict, "wait_time_seconds", arg_dict_wait );
		 Py_DECREF( arg_dict_wait );
		 }
		 */
		msgs = PyObject_Call( get_messages_f, arg_list, arg_dict );
		
		Py_DECREF( arg_list );
		Py_DECREF( arg_dict );
	}
	Py_DECREF( get_messages_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "sqs.get_messages threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( msgs != NULL ) {
			Py_DECREF( msgs );
		}
		return false;
	}
	if( msgs == NULL ) {
		fprintf( stderr, "sqs.get_messages failed\n" );
		return false;
	}
	
	if( PyList_Size( msgs ) < 1 ) {
		// no messages
		return false;
	}
	
	PyObject *msg = PyList_GET_ITEM( msgs, 0 );
	
	PyObject *get_body_f = PyObject_GetAttrString( msg, "get_body" );
	if( get_body_f == NULL ) {
		fprintf( stderr, "boto message does not have get_body\n" );
		return false;
	}
	
	PyObject *bod;
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		bod = PyObject_Call( get_body_f, arg_list, NULL );
		
		Py_DECREF( arg_list );
	}
	Py_DECREF( get_body_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "message.get_body threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( bod != NULL ) {
			Py_DECREF( bod );
		}
		Py_DECREF( msg );
		return false;
	}
	if( bod == NULL ) {
		fprintf( stderr, "message.get_body failed\n" );
		Py_DECREF( msg );
		return false;
	}
	
	body = PyString_AsString( bod );
	Py_DECREF( bod );
	*handle = (void *) msg;
	
	return true;
}
static bool sqs_delete( const std::string &queue_name, void **handle ) throw( ) {
	(void) queue_name;
	
	PyObject *queue = sqs_prep( queue_name );
	if( queue == NULL || *handle == NULL ) {
		return false;
	}
	
	PyObject *o = *((PyObject **) handle);
	
	PyObject *delete_f = PyObject_GetAttrString( o, "delete" );
	if( delete_f == NULL ) {
		fprintf( stderr, "boto message does not have delete\n" );
		Py_DECREF( o );
		return false;
	}
	
	PyObject *ret;
	{
		PyObject *arg_list = PyTuple_New( 0 );
		
		ret = PyObject_Call( delete_f, arg_list, NULL );
		
		Py_DECREF( arg_list );
	}
	Py_DECREF( delete_f );
	
	if( PyErr_Occurred( ) != NULL ) {
		PyObject *ex_type;
		PyObject *ex_value;
		PyObject *ex_traceback;
		PyErr_Fetch( &ex_type, &ex_value, &ex_traceback );
		PyObject *ex_type_name = PyObject_Str( ex_type );
		PyObject *ex_value_str = PyObject_Str( ex_value );
		fprintf( stderr, "message.delete threw exception: %s (%s)", PyString_AsString( ex_type_name ), PyString_AsString( ex_value_str ) );
		Py_DECREF( ex_type_name );
		Py_DECREF( ex_value_str );
		Py_DECREF( ex_type );
		if( ex_value != NULL ) { Py_DECREF( ex_value ); }
		if( ex_traceback != NULL ) { Py_DECREF( ex_traceback ); }
		if( ret != NULL ) {
			Py_DECREF( ret );
		}
		Py_DECREF( o );
		return false;
	}
	if( ret == NULL ) {
		fprintf( stderr, "message.delete failed\n" );
		Py_DECREF( o );
		return false;
	}
	
	Py_DECREF( ret );
	Py_DECREF( o );
	
	return true;
}


static inline std::size_t aws_base64( const unsigned char *const string, const std::size_t bytecount, char *const output, const char characters[64], bool cap, bool term ) throw( ) {
	if( string == NULL ) {
		return 0;
	}
	
	if( output == NULL ) {
		return ((bytecount + 2) / 3) * 4 + (cap ? 0 : (((bytecount + 2) % 3) - 2));
	}
	if( characters == NULL ) {
		characters = BASE64_DEFAULT_ALPHABET;
	}
	
	std::size_t p = 0;
	std::size_t i = 0;
	for( ; i + 2 < bytecount; i += 3, p += 4 ) {
		output[p  ] = characters[string[i]>>2];
		output[p+1] = characters[((string[i]&3)<<4)|(string[i+1]>>4)];
		output[p+2] = characters[((string[i+1]&15)<<2)|(string[i+2]>>6)];
		output[p+3] = characters[string[i+2]&63];
	}
	switch( bytecount % 3 ) {
		case 0:
			if( term ) {
				output[p  ] = '\0';
			}
			return p;
			break;
		case 1:
			output[p  ] = characters[string[i]>>2];
			output[p+1] = characters[(string[i]&3)<<4];
			if( cap ) {
				output[p+2] = '='; output[p+3] = '=';
				if( term ) {
					output[p+4] = '\0';
				}
				return p + 4;
			} else {
				if( term ) {
					output[p+2] = '\0';
				}
				return p + 2;
			}
			break;
		case 2:
			output[p  ] = characters[string[i]>>2];
			output[p+1] = characters[((string[i]&3)<<4)|(string[i+1]>>4)];
			output[p+2] = characters[(string[i+1]&15)<<2];
			if( cap ) {
				output[p+3] = '=';
				if( term ) {
					output[p+4] = '\0';
				}
				return p + 4;
			} else {
				if( term ) {
					output[p+3] = '\0';
				}
				return p + 3;
			}
			break;
		default:
			assert( 0 ); // impossible
			break;
	}
}
static inline std::size_t aws_unbase64( const unsigned char *const string, const std::size_t length, char *const output, const char characters[64], bool term ) throw( ) {
	if( string == NULL ) {
		return 0;
	}
	
	if( output == NULL ) {
		return (length * 3) / 4 + (term ? 1 : 0); // = upper limit (ignores cap)
	}
	if( characters == NULL ) {
		characters = BASE64_DEFAULT_ALPHABET;
	}
	
	static unsigned char tbl[256];
	memset( tbl, 0, 256 * sizeof( unsigned char ) );
	for( int i = 0; i < 64; ++ i ) {
		tbl[(unsigned char)characters[i]] = (unsigned char) i;
	}
	
	std::size_t p = 0;
	std::size_t i = 0;
	for( ; i + 5 < length; i += 4, p += 3 ) {
		const unsigned char v0 = tbl[string[i  ]];
		const unsigned char v1 = tbl[string[i+1]];
		const unsigned char v2 = tbl[string[i+2]];
		const unsigned char v3 = tbl[string[i+3]];
		
		// 11111111 22222222 33333333
		// aaaaaabb bbbbcccc ccdddddd
		
		output[p  ] = (char) ((v0 << 2) | (v1 >> 4));
		output[p+1] = (char) ((v1 << 4) | (v2 >> 2));
		output[p+2] = (char) ((v2 << 6) | v3);
	}
	
	if( i + 2 > length ) {
		if( term ) {
			output[p] = '\0';
		}
		return p;
	}
	
	const unsigned char v0 = tbl[string[i  ]];
	const unsigned char v1 = tbl[string[i+1]];
	if( v0 == 0 && string[i  ] != characters[0] ) {
		if( term ) {
			output[p] = '\0';
		}
		return p;
	} else if( v1 == 0 && string[i+1] != characters[0] ) {
		fprintf( stderr, "unbase64: not a valid format\n" );
		if( term ) {
			output[p] = '\0';
		}
		return p;
	}
	
	output[p  ] = (char) ((v0 << 2) | (v1 >> 4));
	
	if( i + 3 > length ) {
		if( term ) {
			output[p+1] = '\0';
		}
		return p + 1;
	}
	const unsigned char v2 = tbl[string[i+2]];
	if( v2 == 0 && string[i+2] != characters[0] ) {
		if( term ) {
			output[p+1] = '\0';
		}
		return p + 1;
	}
	
	output[p+1] = (char) ((v1 << 4) | (v2 >> 2));
	
	if( i + 4 > length ) {
		if( term ) {
			output[p+2] = '\0';
		}
		return p + 2;
	}
	const unsigned char v3 = tbl[string[i+3]];
	if( v3 == 0 && string[i+3] != characters[0] ) {
		if( term ) {
			output[p+2] = '\0';
		}
		return p + 2;
	}
	
	output[p+2] = (char) ((v2 << 6) | v3);
	
	if( term ) {
		output[p+3] = '\0';
	}
	return p + 3;
}

#endif
