// botoc_common.h: common functions for all BOTO communication
// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

#ifndef BOTOC_COMMON_H_INCLUDED__
#define BOTOC_COMMON_H_INCLUDED__

/* standard libraries */

#include <vector>
#include <string>
#include <map>

/* enable fancy compiler extras if they are available */

#if defined(__GNUC__) && __GNUC__ > 0
#  define likely(x) __builtin_expect(!!(x),1)
#  define unlikely(x) __builtin_expect(!!(x),0)
#else
#  define __attribute__(x)
#  define likely(x) x
#  define unlikely(x) x
#endif

#define LOCALBLOCK

/* constants */

#define SIZED_STRING(s) (int)(s).size(),(s).data()
#define BASE64_DEFAULT_ALPHABET "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


namespace botoc {
	/* typedefs */
	
	/* If you want to use your own string/vector classes, this is the place to
	 * specify them (they must conform to the standard library interface) */
	
	typedef std::size_t size_t;
	typedef std::string string_t;
	typedef const std::string const_string_t;
	typedef std::vector<string_t> string_list_t;
	typedef void *handle_t; // used to return python objects as handles
	
	/* globals */
	
	static string_t user_key;
	static string_t user_secret;
	static string_t region;
	
	/* prototypes */
	
	// Configuration
	__attribute__((warn_unused_result,unused))
	static inline bool set_iam_user( const const_string_t &key, const const_string_t &secret ) throw( );
	
	__attribute__((warn_unused_result,unused))
	static inline bool set_region( const const_string_t &region ) throw( );
	
	// Base64
	__attribute__((warn_unused_result,unused))
	static inline size_t base64( const unsigned char *string, size_t bytecount, char *output, const char alphabet[64] = NULL, bool cap = true, bool term = true ) throw( );
	
	__attribute__((warn_unused_result,unused))
	static inline size_t unbase64( const unsigned char *string, size_t length, char *output, const char alphabet[64] = NULL, bool term = false ) throw( );
	
	__attribute__((warn_unused_result,unused))
	static inline bool encode_binary( const void *data, size_t length, string_t &output ) throw( );
	
	__attribute__((warn_unused_result,unused))
	static inline size_t decode_binary( const const_string_t &data, void **output ) throw( );
	
	// Python helpers
	__attribute__((always_inline,warn_unused_result,unused))
	static inline bool py_init( void ) throw( );
	
	__attribute__((always_inline,unused))
	static inline void py_release( PyObject *o ) throw( );
	
	__attribute__((always_inline,warn_unused_result,unused))
	static inline bool py_release_success( PyObject *o ) throw( );
	
	__attribute__((always_inline,warn_unused_result,unused))
	static inline PyObject *py_string( const char *str ) throw( );
	
	__attribute__((always_inline,warn_unused_result,unused))
	static inline PyObject *py_string( const char *str, size_t size ) throw( );
	
	__attribute__((always_inline,warn_unused_result,unused))
	static inline PyObject *py_string( const const_string_t &str ) throw( );
	
	__attribute__((always_inline,warn_unused_result,unused))
	static inline const char *py_cstring( PyObject *str ) throw( );
	
	__attribute__((warn_unused_result,unused))
	static inline PyObject *py_listitem_tmp( PyObject *list, Py_ssize_t index ) throw( );
	
	__attribute__((warn_unused_result))
	static bool py_error( const char *stage, const char *extra = "" ) throw( );
	
	__attribute__((warn_unused_result))
	static PyObject *py_import( const char *path ) throw( );
	
	__attribute__((warn_unused_result,sentinel))
	static PyObject *py_construct( PyObject *space, const char *classname, ... ) throw( );
	
	__attribute__((warn_unused_result,sentinel))
	static PyObject *py_callfunc( PyObject *object, const char *funcname, ... ) throw( );
	
	/* implementation */
	
	// Configuration
	static inline bool set_iam_user( const const_string_t &key, const const_string_t &secret ) throw( ) {
		try {
			user_key.assign( key );
			user_secret.assign( secret );
		} catch( ... ) {
			return false;
		}
		return true;
	}
	
	static inline bool set_region( const const_string_t &reg ) throw( ) {
		try {
			region.assign( reg );
		} catch( ... ) {
			return false;
		}
		return true;
	}
	
	// Base64
	static inline size_t base64( const unsigned char *const string, const size_t bytecount, char *const output, const char alphabet[64], const bool cap, const bool term ) throw( ) {
		if( unlikely( string == NULL ) ) {
			return 0;
		}
		
		if( output == NULL ) {
			return ((bytecount + 2) / 3) * 4 + (cap ? 0 : (((bytecount + 2) % 3) - 2));
		}
		if( alphabet == NULL ) {
			alphabet = BASE64_DEFAULT_ALPHABET;
		}
		
		size_t p = 0;
		size_t i = 0;
		for( ; i + 2 < bytecount; i += 3, p += 4 ) {
			output[p  ] = alphabet[string[i]>>2];
			output[p+1] = alphabet[((string[i]&3)<<4)|(string[i+1]>>4)];
			output[p+2] = alphabet[((string[i+1]&15)<<2)|(string[i+2]>>6)];
			output[p+3] = alphabet[string[i+2]&63];
		}
		switch( bytecount % 3 ) {
			case 0:
				if( term ) {
					output[p  ] = '\0';
				}
				return p;
				break;
			case 1:
				output[p  ] = alphabet[string[i]>>2];
				output[p+1] = alphabet[(string[i]&3)<<4];
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
				output[p  ] = alphabet[string[i]>>2];
				output[p+1] = alphabet[((string[i]&3)<<4)|(string[i+1]>>4)];
				output[p+2] = alphabet[(string[i+1]&15)<<2];
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
				// impossible
#ifdef assert
				assert( 0 );
#endif
				break;
		}
	}
	static inline size_t unbase64( const unsigned char *const string, const size_t length, char *const output, const char alphabet[64], const bool term ) throw( ) {
		if( unlikely( string == NULL ) ) {
			return 0;
		}
		
		if( output == NULL ) {
			return (length * 3) / 4 + (term ? 1 : 0); // = upper limit (ignores cap)
		}
		if( alphabet == NULL ) {
			alphabet = BASE64_DEFAULT_ALPHABET;
		}
		
		static unsigned char tbl[256];
		memset( tbl, 0, 256 * sizeof( unsigned char ) );
		for( int i = 0; i < 64; ++ i ) {
			tbl[(unsigned char)alphabet[i]] = (unsigned char) i;
		}
		
		size_t p = 0;
		size_t i = 0;
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
		if( v0 == 0 && string[i  ] != alphabet[0] ) {
			if( term ) {
				output[p] = '\0';
			}
			return p;
		} else if( v1 == 0 && string[i+1] != alphabet[0] ) {
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
		if( v2 == 0 && string[i+2] != alphabet[0] ) {
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
		if( v3 == 0 && string[i+3] != alphabet[0] ) {
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
	
	static inline bool encode_binary( const void *const data, const size_t length, string_t &output ) throw( ) {
		size_t l = base64( (const unsigned char *) data, length, NULL, NULL, true, false );
		try {
			output.resize( l ); // slower than reserve, but std::string optimises weirdly otherwise :(
		} catch( ... ) {
			fprintf( stderr, "encode_binary: reserve failed\n" );
			return false;
		}
		
		// this is not 100% safe in C++03 (but I think is in C++11)
		// technically we're not allowed to manipulate .data(),
		// but it seems to be fine in all implementations I've seen
		char *const internal = const_cast<char *>( output.data( ) );
		l = base64( (const unsigned char *) data, length, internal, NULL, true, false );
		try {
			output.resize( l ); // actual size may be less
		} catch( ... ) {
			fprintf( stderr, "encode_binary: resize failed (!)\n" );
			return false;
		}
		return true;
	}
	
	static inline size_t decode_binary( const const_string_t &data, void **output ) throw( ) {
		if( unlikely( output == NULL ) ) {
			return 0;
		}
		
		const unsigned char *const d = (const unsigned char *) data.data( );
		const size_t length = data.size( );
		
		const size_t l = unbase64( d, length, NULL, NULL, false );
		if( unlikely( l == 0 ) ) {
			*output = NULL;
			return 0;
		}
		
		*output = malloc( l );
		if( unlikely( *output == NULL ) ) {
			fprintf( stderr, "decode_binary: malloc failed\n" );
			return 0;
		}
		
		return unbase64( d, length, (char *) *output, NULL, false );
	}
	
	// Python helpers
	static inline bool py_init( void ) throw( ) {
		Py_Initialize( );
		return !py_error( "initializing python" );
	}
	
	static inline void py_release( PyObject *o ) throw( ) {
		if( likely( o != NULL ) ) {
			Py_DECREF( o );
		}
	}
	
	static inline bool py_release_success( PyObject *o ) throw( ) {
		if( unlikely( o == NULL ) ) {
			return false;
		}
		Py_DECREF( o );
		return true;
	}
	
	static inline PyObject *py_string( const char *const str ) throw( ) {
		return PyString_FromString( str );
	}
	
	static inline PyObject *py_string( const char *const str, const size_t size ) throw( ) {
		return PyString_FromStringAndSize( str, (Py_ssize_t) size );
	}
	
	static inline PyObject *py_string( const const_string_t &str ) throw( ) {
		return PyString_FromStringAndSize( str.data( ), (Py_ssize_t) str.size( ) );
	}
	
	static inline const char *py_cstring( PyObject *str ) throw( ) {
		return PyString_AsString( str );
	}
	
	static inline PyObject *py_listitem_tmp( PyObject *list, Py_ssize_t index ) throw( ) {
		if( unlikely( list == NULL ) ) {
			return NULL;
		}
		if( index >= PyList_Size( list ) || index < 0 ) {
			Py_DECREF( list );
			return NULL;
		}
		PyObject *itm = PyList_GET_ITEM( list, index );
		if( unlikely( itm == NULL ) ) {
			Py_DECREF( list );
			return NULL;
		}
		Py_INCREF( itm );
		Py_DECREF( list );
		return itm;
	}
	
	static bool py_error( const char *const stage, const char *const extra ) throw( ) {
		if( likely( PyErr_Occurred( ) == NULL ) ) {
			return false;
		}
		PyObject *type;
		PyObject *value;
		PyObject *traceback;
		PyErr_Fetch( &type, &value, &traceback );
		if( likely( value != NULL ) ) {
			PyObject *value_str = PyObject_Str( value );
			fprintf( stderr, "botoc: %s%s threw %s\n", stage, extra, py_cstring( value_str ) );
			py_release( value_str );
		} else {
			PyObject *type_name = PyObject_Str( type );
			fprintf( stderr, "botoc: %s%s threw %s\n", stage, extra, py_cstring( type_name ) );
			py_release( type_name );
		}
		py_release( type );
		py_release( value );
		py_release( traceback );
		return true;
	}
	
	static PyObject *py_import( const char *const path ) throw( ) {
		if( unlikely( path == NULL ) ) {
			return NULL;
		}
		PyObject *path_str = py_string( path );
		PyObject *module = PyImport_Import( path_str );
		Py_DECREF( path_str );
		if( unlikely( py_error( "Import ", path ) ) ) {
			py_release( module );
			return NULL;
		}
		if( unlikely( module == NULL ) ) {
			fprintf( stderr, "python module %s not found\n", path );
			return NULL;
		}
		PyObject *space = PyModule_GetDict( module ); // borrowed
		if( unlikely( py_error( "dictionary for module ", path ) ) ) {
			Py_DECREF( module );
			return NULL;
		}
		if( unlikely( space == NULL ) ) {
			fprintf( stderr, "python module %s has no dictionary\n", path );
			Py_DECREF( module );
			return NULL;
		}
		return module; // the module has ownership of the dictionary
	}
	
#define py_cancel_va( param ) \
	va_list v2; \
	va_start( v2, param ); \
	while( 1 ) { \
		const char *n = va_arg( v2, const char* ); \
		if( unlikely( n == NULL ) ) { \
			break; \
		} \
		PyObject *o = va_arg( v2, PyObject* ); \
		py_release( o ); \
	} \
	va_end( v2 )
	
#define py_call_va( func, param, fail ) \
	va_list v; \
	va_start( v, param ); \
	int args = 0; \
	bool bad = false; \
	bool usesdict = false; \
	while( 1 ) { \
		const char *n = va_arg( v, const char* ); \
		if( unlikely( n == NULL ) ) { \
			break; \
		} \
		if( n[0] == '-' ) { \
			(void) va_arg( v, PyObject* ); \
			continue; \
		} \
		if( n[0] == '\0' ) { \
			++ args; \
		} else { \
			usesdict = true; \
		} \
		PyObject *o = va_arg( v, PyObject* ); \
		if( unlikely( o == NULL ) ) { \
			bad = true; \
			break; \
		} \
	} \
	va_end( v ); \
	if( unlikely( bad ) ) { \
		py_cancel_va( param ); \
		fail \
	} \
	PyObject *arg_list = PyTuple_New( (Py_ssize_t) args ); \
	PyObject *arg_dict = usesdict ? PyDict_New( ) : NULL; \
	va_start( v, param ); \
	args = 0; \
	while( 1 ) { \
		const char *n = va_arg( v, const char* ); \
		if( unlikely( n == NULL ) ) { \
			break; \
		} \
		PyObject *o = va_arg( v, PyObject* ); \
		if( n[0] == '-' ) { \
			py_release( o ); \
		} else if( n[0] == '\0' ) { \
			PyTuple_SET_ITEM( arg_list, args, o ); \
			++ args; \
		} else { \
			PyDict_SetItemString( arg_dict, n, o ); \
			Py_DECREF( o ); \
		} \
	} \
	va_end( v ); \
	PyObject *ret = PyObject_Call( func, arg_list, arg_dict ); \
	Py_DECREF( arg_list ); \
	py_release( arg_dict )
	
	static PyObject *py_construct( PyObject *mod, const char *const cls, ... ) throw( ) {
		if( unlikely( mod == NULL || cls == NULL ) ) {
			return NULL;
		}
		
		PyObject *dict = PyModule_GetDict( mod ); // borrowed
		if( unlikely( py_error( "dictionary containing ", cls ) ) ) {
			return NULL;
		}
		if( unlikely( dict == NULL ) ) {
			fprintf( stderr, "python module for %s has no dictionary\n", cls );
			return NULL;
		}
		
		PyObject *classobj = PyDict_GetItemString( dict, cls );
		if( unlikely( py_error( "find constructor ", cls ) ) ) {
			py_release( classobj );
			py_cancel_va( cls );
			return NULL;
		}
		if( unlikely( classobj == NULL ) ) {
			fprintf( stderr, "python class %s not found\n", cls );
			py_cancel_va( cls );
			return NULL;
		}
		py_call_va( classobj, cls, Py_DECREF( classobj ); return NULL; );
		Py_DECREF( classobj );
		if( unlikely( py_error( cls, " constructor" ) ) ) {
			py_release( ret );
			return NULL;
		}
		if( unlikely( ret == NULL ) ) {
			fprintf( stderr, "python class %s not created\n", cls );
			return NULL;
		}
		
		return ret;
	}
	
	static PyObject *py_callfunc( PyObject *obj, const char *const fnc, ... ) throw( ) {
		if( unlikely( obj == NULL || fnc == NULL ) ) {
			return NULL;
		}
		
		if( unlikely( !PyObject_HasAttrString( obj, fnc ) ) ) {
			fprintf( stderr, "python function %s not found\n", fnc );
			return NULL;
		}
		PyObject *funcobj = PyObject_GetAttrString( obj, fnc );
		if( unlikely( py_error( "find function ", fnc ) ) ) {
			py_release( funcobj );
			py_cancel_va( fnc );
			return NULL;
		}
		if( unlikely( funcobj == NULL ) ) {
			fprintf( stderr, "python function %s error\n", fnc );
			py_cancel_va( fnc );
			return NULL;
		}
		py_call_va( funcobj, fnc, Py_DECREF( funcobj ); return NULL; );
		Py_DECREF( funcobj );
		if( unlikely( py_error( fnc ) ) ) {
			py_release( ret );
			return NULL;
		}
		if( unlikely( ret == NULL ) ) {
			fprintf( stderr, "python function %s failed\n", fnc );
			return NULL;
		}
		
		return ret;
	}
	
#undef py_cancel_va
#undef py_call_va
}

#undef BASE64_DEFAULT_ALPHABET

#endif
