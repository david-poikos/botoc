// botoc_ddb.h: DDB communication wrapper
// Copyright 2013, Poikos Ltd.
// Author: David Evans
// Based on code from 9apps

// usage:
//  1: include python (first!)
//  2: include this header
//  3: call botoc::set_iam_user( key, secret ) and botoc::set_region( region )
//  4: use as required:
//       botoc::ddb::update( table, key, items[, expected] )
//       botoc::ddb::get( table, key, consistent, items )
//       botoc::ddb::disconnect( )
//  5: link with python

// changing iam user or region after performing an action has no effect; calling
// disconnect() will close the current connection, and the next request will use
// the new credentials / url

#ifndef BOTOC_DDB_H_INCLUDED__
#define BOTOC_DDB_H_INCLUDED__

#include "botoc_common.h"

namespace botoc {
	namespace ddb {
		/* constants */
		
		enum data_type {
			UNKNOWN   = 3,
			SET       = 0x4,
			
			STRING    = 0,      // S
			NUMBER    = 1,      // N
			BINARY    = 2,      // B
			STRINGSET = STRING|SET, // SS
			NUMBERSET = NUMBER|SET, // NS
			BINARYSET = BINARY|SET  // BS
		};
		enum flag_raw {
			RAW
		};
		enum data_action {
			REPLACE  = 0,
			ADD      = 1,
			DELETE   = 2
		};
		
		/* prototypes */
		
		__attribute__((const,warn_unused_result,always_inline))
		static inline const char *string_from_type( const data_type type ) _noexcept;
		
		__attribute__((const,warn_unused_result,always_inline))
		static inline const char *string_from_action( const data_action action ) _noexcept;
		
		__attribute__((pure,warn_unused_result,always_inline))
		static inline data_type type_from_string( const char *const type ) _noexcept;
		
		/* implementation */
		
		static inline const char *string_from_type( const data_type type ) _noexcept {
			return &("S\0\0N\0\0B\0\0\0\0\0SS\0NS\0BS"[type*3]);
		}
		static inline const char *string_from_action( const data_action action ) _noexcept {
			return &("PUT\0ADD\0DELETE"[action*4]);
		}
		static inline data_type type_from_string( const char *const type ) _noexcept {
			if( unlikely( type == NULL ) ) {
				return UNKNOWN;
			}
			switch( type[0] ) {
				case 'B':
					return type[1] == 'S' ? BINARYSET : BINARY;
					break;
				case 'N':
					return type[1] == 'S' ? NUMBERSET : NUMBER;
					break;
				case 'S':
					return type[1] == 'S' ? STRINGSET : STRING;
					break;
				default:
					return UNKNOWN;
					break;
			}
		}
		
		/* classes */
		
		class item {
		private:
			string_t _name;
			data_type _type;
			data_action _action;
			union union_t {
#if ALLOW_NONPOD_UNION
				string_t value;
				string_list_t list;
#else
				char value[sizeof(string_t)];
				char list[sizeof(string_list_t)];
#endif
				inline union_t( void ) _noexcept { }
				inline union_t( const union_t& ) _noexcept { }
				inline ~union_t( void ) _noexcept { }
			} _data;
			
		public:
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_t &name( void ) const _noexcept {
				return _name;
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline data_type type( void ) const _noexcept {
				return _type;
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const char *type_string( void ) const _noexcept {
				return string_from_type( _type );
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline data_action action( void ) const _noexcept {
				return _action;
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const char *action_string( void ) const _noexcept {
				return string_from_action( _action );
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_t *value( void ) const _noexcept {
				return ((_type & SET) || _type == UNKNOWN) ?
#if ALLOW_NONPOD_UNION
				NULL : &_data.value;
#else
				NULL : ((const string_t *) &_data.value);
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline string_t &_value( void ) _noexcept {
#if ALLOW_NONPOD_UNION
				return _data.value;
#else
				return *((string_t *) &_data.value);
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_t &_value( void ) const _noexcept {
#if ALLOW_NONPOD_UNION
				return _data.value;
#else
				return *((const string_t *) &_data.value);
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_t &value_knowntype( void ) const _noexcept {
				return _value( );
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_list_t *list( void ) const _noexcept {
				return ((_type & SET) && _type != UNKNOWN) ?
#if ALLOW_NONPOD_UNION
				&_data.list : NULL;
#else
				((const string_list_t *) &_data.list) : NULL;
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline string_list_t &_list( void ) _noexcept {
#if ALLOW_NONPOD_UNION
				return _data.list;
#else
				return *((string_list_t *) &_data.list);
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_list_t &_list( void ) const _noexcept {
#if ALLOW_NONPOD_UNION
				return _data.list;
#else
				return *((const string_list_t *) &_data.list);
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline const string_list_t &list_knowntype( void ) const _noexcept {
#if ALLOW_NONPOD_UNION
				return _data.list;
#else
				return _list( );
#endif
			}
			
			__attribute__((pure,warn_unused_result,always_inline))
			inline size_t size( void ) const _noexcept {
				return unlikely( _type == UNKNOWN ) ? 0 :
				((_type & SET) ? _list( ).size( ) : _value( ).size( ));
			}
			
			/*
			 inline float float_value( void ) const _noexcept {
			 if( _type != NUMBER ) {
			 return 0.0f / 0.0f;
			 }
			 float r;
			 if( snscanf( _value, _length, "%f", &r ) == 1 ) {
			 return r;
			 } else {
			 return 0.0f / 0.0f;
			 }
			 }
			 inline double double_value( void ) const _noexcept {
			 if( _type != NUMBER ) {
			 return 0.0 / 0.0;
			 }
			 double r;
			 if( snscanf( _value, _length, "%lf", &r ) == 1 ) {
			 return r;
			 } else {
			 return 0.0 / 0.0;
			 }
			 }
			 inline int int_value( void ) const _noexcept {
			 if( _type != NUMBER ) {
			 return 0;
			 }
			 int r;
			 if( snscanf( _value, _length, "%d", &r ) == 1 ) {
			 return r;
			 } else {
			 return 0;
			 }
			 }
			 inline long long_value( void ) const _noexcept {
			 if( _type != NUMBER ) {
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
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_name( const const_string_t &name ) _noexcept {
				try {
					_name.assign( name );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_type( data_type type ) _noexcept {
				if( _type == type ) {
					return true;
				}
				if( type == UNKNOWN ) {
					if( (_type & SET) ) {
						_list( ).~string_list_t( );
					} else {
						_value( ).~string_t( );
					}
				} else if( _type == UNKNOWN ) {
					if( (type & SET) ) {
						try {
							(void) new( &_list( ) ) string_list_t( );
						} catch( ... ) { return false; }
					} else {
						try {
							(void) new( &_value( ) ) string_t( );
						} catch( ... ) { return false; }
					}
				} else if( (_type & SET) != (type & SET) ) {
					if( (type & SET) ) {
						_value( ).~string_t( );
						try {
							(void) new( &_list( ) ) string_list_t( );
						} catch( ... ) {
							_type = UNKNOWN;
							return false;
						}
					} else {
						_list( ).~string_list_t( );
						try {
							(void) new( &_value( ) ) string_t( );
						} catch( ... ) {
							_type = UNKNOWN;
							return false;
						}
					}
				}
				_type = type;
				return true;
			}
			
			__attribute__((always_inline))
			inline void set_action( data_action action ) _noexcept {
				_action = action;
			}
			
			__attribute__((always_inline))
			inline void clear_data( void ) _noexcept {
				(void) set_type( UNKNOWN );
			}
			
			__attribute__((always_inline))
			inline void clear( void ) _noexcept {
				_name.clear( );
				clear_data( );
				_action = REPLACE;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const const_string_t &value ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				try {
					_value( ).assign( value );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const const_string_t &value, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				try {
					_value( ).assign( value );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const char *value ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				try {
					_value( ).assign( value );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const char *value, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				try {
					_value( ).assign( value );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const char *value, size_t length ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				try {
					_value( ).assign( value, length );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( const char *value, size_t length, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				try {
					_value( ).assign( value, length );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const const_string_t &value ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value.data( ), value.size( ), _value( ) );
				} else {
					try {
						_value( ).assign( value );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const char *value ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value, (size_t) strlen( value ), _value( ) );
				} else {
					try {
						_value( ).assign( value );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const void *value, size_t length ) _noexcept {
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value, length, _value( ) );
				} else {
					try {
						_value( ).assign( (const char *) value, length );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const const_string_t &value, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value.data( ), value.size( ), _value( ) );
				} else {
					try {
						_value( ).assign( value );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const char *value, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value, (size_t) strlen( value ), _value( ) );
				} else {
					try {
						_value( ).assign( value );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_binary( const void *value, size_t length, data_type type ) _noexcept {
				if( unlikely( type == UNKNOWN || (type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				if( _type == BINARY ) {
					return encode_binary( value, length, _value( ) );
				} else {
					try {
						_value( ).assign( (const char *) value, length );
					} catch( ... ) { return false; }
					return true;
				}
			}
			
			__attribute__((warn_unused_result,format(printf,2,3)))
			inline bool set_value_format( const char *format, ... ) _noexcept {
				va_list v;
				
				if( unlikely( _type == UNKNOWN || (_type & SET) ) ) {
					return false;
				}
				
				va_start( v, format );
				const int sz = vsnprintf( NULL, 0, format, v ) + 1;
				va_end( v );
				
				if( unlikely( sz < 0 ) ) {
					_value( ).clear( );
					return true;
				}
				
				try {
					_value( ).resize( (size_t) sz ); // slower than reserve, but std::string optimises weirdly otherwise :(
				} catch( ... ) {
					fprintf( stderr, "set_value_format: reserve failed\n" );
					return false;
				}
				
				va_start( v, format );
				const size_t l = (size_t) vsnprintf( const_cast<char *>( _value( ).data( ) ), (size_t) sz, format, v );
				va_end( v );
				
				try {
					_value( ).resize( l );
				} catch( ... ) {
					fprintf( stderr, "set_value_format: resize failed (!)\n" );
					return false;
				}
				
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( float value, data_action action = REPLACE ) _noexcept {
				if( unlikely( _type != NUMBER ) ) {
					return false;
				}
				set_action( action );
				return set_value_format( "%f", value );
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( double value, data_action action = REPLACE ) _noexcept {
				if( unlikely( _type != NUMBER ) ) {
					return false;
				}
				set_action( action );
				return set_value_format( "%f", value );
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( int value, data_action action = REPLACE ) _noexcept {
				if( unlikely( _type != NUMBER ) ) {
					return false;
				}
				set_action( action );
				return set_value_format( "%d", value );
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool set_value( long value, data_action action = REPLACE ) _noexcept {
				if( unlikely( _type != NUMBER ) ) {
					return false;
				}
				set_action( action );
				return set_value_format( "%ld", value );
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool add_item( const const_string_t &value ) _noexcept {
				if( unlikely( !(_type & SET) ) ) {
					return false;
				}
				try {
					_list( ).push_back( string_t( value ) );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool add_item( const char *value ) _noexcept {
				if( unlikely( !(_type & SET) ) ) {
					return false;
				}
				try {
					_list( ).push_back( string_t( value ) );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool add_item( const char *value, size_t length ) _noexcept {
				if( unlikely( !(_type & SET) ) ) {
					return false;
				}
				try {
					_list( ).push_back( string_t( value, length ) );
				} catch( ... ) { return false; }
				return true;
			}
			
			__attribute__((always_inline))
			inline void clear_items( void ) _noexcept {
				if( _type & SET ) {
					_list( ).clear( );
				}
			}
			
			__attribute__((always_inline,warn_unused_result))
			inline bool clear_items( data_type type ) _noexcept {
				if( unlikely( !(type & SET) ) ) {
					return false;
				}
				if( unlikely( !set_type( type ) ) ) {
					return false;
				}
				_list( ).clear( );
				return true;
			}
			
			__attribute__((always_inline))
			inline item( void ) _noexcept :
			_name( ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
			}
			
			__attribute__((always_inline))
			inline explicit item( const const_string_t &name, data_action action = REPLACE ) _noexcept :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
			}
			
			__attribute__((always_inline))
			inline explicit item( const char *name, data_action action = REPLACE ) _noexcept :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, data_type type, data_action action = REPLACE ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
				if( unlikely( !set_type( type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, const const_string_t &value, data_type type = STRING ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				if( unlikely( !set_value( value, type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, const char *value, data_type type = STRING ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				if( unlikely( !set_value( value, type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, const char *value, size_t length, data_type type = STRING ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				if( unlikely( !set_binary( value, length, type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, const char *value, size_t length, flag_raw raw, data_type type = BINARY ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				(void) raw;
				if( unlikely( !set_value( value, length, type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, const void *value, size_t length, data_type type = BINARY ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				if( unlikely( !set_binary( value, length, type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, float value, data_action action = REPLACE ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( REPLACE ),
			_data( )
			{
				if( unlikely( !set_type( NUMBER ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				if( unlikely( !set_value( value, action ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, double value, data_action action = REPLACE ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
				if( unlikely( !set_type( NUMBER ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				if( unlikely( !set_value( value, action ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, int value, data_action action = REPLACE ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
				if( unlikely( !set_type( NUMBER ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				if( unlikely( !set_value( value, action ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const const_string_t &name, long value, data_action action = REPLACE ) throw( std::bad_alloc ) :
			_name( name ),
			_type( UNKNOWN ),
			_action( action ),
			_data( )
			{
				if( unlikely( !set_type( NUMBER ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				if( unlikely( !set_value( value, action ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
			}
			
			__attribute__((always_inline))
			inline item( const item &copy ) throw( std::bad_alloc ) :
			_name( copy._name ),
			_type( UNKNOWN ),
			_action( copy._action ),
			_data( )
			{
				if( unlikely( !set_type( copy._type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				if( copy._type != UNKNOWN ) {
					if( (copy._type & SET) ) {
						_list( ) = copy._list( );
					} else {
						_value( ).assign( copy._value( ) );
					}
				}
			}
			
			__attribute__((always_inline))
			inline item &operator =( const item &copy ) throw( std::bad_alloc ) {
				_name.assign( copy._name );
				if( unlikely( !set_type( copy._type ) ) ) {
					std::bad_alloc ex;
					throw ex;
				}
				_action = copy._action;
				if( copy._type != UNKNOWN ) {
					if( (copy._type & SET) ) {
						_list( ) = copy._list( );
					} else {
						_value( ).assign( copy._value( ) );
					}
				}
				return *this;
			}
			
			__attribute__((always_inline))
			inline ~item( void ) _noexcept {
				clear_data( );
			}
		};
		
		typedef std::vector<item> item_list_t;
		
		/* prototypes */
		
		__attribute__((warn_unused_result,unused))
		static bool update( const const_string_t &db, const const_string_t &key, const item_list_t &items, const item_list_t *expected = NULL ) _noexcept;
		
		__attribute__((warn_unused_result,unused))
		static bool get( const const_string_t &db, const const_string_t &key, bool consistent, item_list_t &items ) _noexcept;
		
		__attribute__((always_inline,unused))
		static inline void disconnect( void ) _noexcept;
		
		/* internal prototypes */
		
		__attribute__((warn_unused_result))
		static PyObject *prep( bool disconnect = false ) _noexcept;
		
		__attribute__((warn_unused_result))
		static PyObject *dict_from_items_expect( const item_list_t &items ) _noexcept;
		
		__attribute__((warn_unused_result))
		static PyObject *dict_from_items_update( const item_list_t &items ) _noexcept;
		
		__attribute__((warn_unused_result))
		static PyObject *list_from_items( const item_list_t &items ) _noexcept;
		
		__attribute__((warn_unused_result))
		static inline bool item_from_dict( PyObject *obj, item &output ) _noexcept;
		
		__attribute__((warn_unused_result))
		static bool update_from_dict( item_list_t &items, PyObject *ret_items ) _noexcept;
		
		/* implementation */
		
		static PyObject *prep( const bool disconnect ) _noexcept {
			/*
			 * import boto.regioninfo
			 * import boto.dynamodb.layer1
			 *
			 * layer1 = boto.dynamodb.layer1.Layer1(
			 *   aws_access_key_id = [key],
			 *   aws_secret_access_key = [secret],
			 *   region = boto.regioninfo.RegionInfo(
			 *     name = [region],
			 *     endpoint = 'dynamodb.' + [region] + '.amazonaws.com'
			 *   )
			 * )
			 */
			
			static bool tried = false;
			static PyObject *layer1 = NULL;
			
			if( disconnect ) {
				if( layer1 == NULL ) {
					return NULL;
				}
				
				py_release( layer1 );
				layer1 = NULL;
				
				tried = false;
				return NULL;
			}
			
			if( tried ) {
				return layer1;
			}
			if( unlikely( region.size( ) <= 0 ) ) {
				fprintf( stderr, "attempted to connect to DDB without a region\n" );
				return NULL;
			}
			if( unlikely( user_key.size( ) <= 0 || user_secret.size( ) <= 0 ) ) {
				fprintf( stderr, "attempted to connect to DDB without a valid IAM user\n" );
				return NULL;
			}
			tried = true;
			
			if( unlikely( !py_init( ) ) ) {
				return NULL;
			}
			
			PyObject *regioninfo_mod = py_import( "boto.regioninfo" );
			PyObject *ddb_mod = py_import( "boto.dynamodb.layer1" );
			
			string_t endpoint( "dynamodb." );
			endpoint.append( region );
			endpoint.append( ".amazonaws.com" );
			
			layer1 = py_construct( ddb_mod, "Layer1",
				"aws_access_key_id", py_string( user_key ),
				"aws_secret_access_key", py_string( user_secret ),
				"region", py_construct( regioninfo_mod, "RegionInfo",
					"name", py_string( region ),
					"endpoint", py_string( endpoint ),
				NULL ),
			NULL );
			
			py_release( regioninfo_mod );
			py_release( ddb_mod );
			
			if( unlikely( layer1 == NULL ) ) {
				fprintf( stderr, "could not connect to DDB\n" );
			}
			
			return layer1;
		}
		
		static PyObject *dict_from_items_expect( const item_list_t &items ) _noexcept {
			PyObject *r = PyDict_New( );
			for( size_t i = 0, e = items.size( ); i < e; ++ i ) {
				const size_t f = items[i].size( );
				if( items[i].type( ) == UNKNOWN ) {
					continue;
				}
				PyObject *t = PyDict_New( );
				if( f == 0 ) {
					PyDict_SetItemString( t, "Exists", Py_False );
				} else {
					PyObject *o = PyDict_New( );
					PyObject *v;
					if( (items[i].type( ) & SET) ) {
						v = PyList_New( (Py_ssize_t) f );
						for( size_t j = 0; j < f; ++ j ) {
							const string_t &s = items[i]._list( )[j];
							if( s.size( ) > 0 ) {
								PyList_SET_ITEM( v, j, py_string( s ) );
							}
						}
					} else {
						v = py_string( items[i]._value( ) );
					}
					PyDict_SetItemString( o, items[i].type_string( ), v );
					Py_DECREF( v );
					PyDict_SetItemString( t, "Value", o );
					Py_DECREF( o );
				}
				PyObject *key = py_string( items[i].name( ) );
				PyDict_SetItem( r, key, t );
				Py_DECREF( key );
				Py_DECREF( t );
			}
			if( unlikely( py_error( "dict_from_items_expect" ) ) ) {
				py_release( r );
				return NULL;
			}
			return r;
		}
		
		static PyObject *dict_from_items_update( const item_list_t &items ) _noexcept {
			PyObject *r = PyDict_New( );
			for( size_t i = 0, e = items.size( ); i < e; ++ i ) {
				const size_t f = items[i].size( );
				PyObject *t = PyDict_New( );
				if( (items[i].action( ) == DELETE && (!(items[i].type( ) & SET) || f <= 0)) || (f <= 0 && items[i].action( ) == REPLACE) ) {
					PyObject *s = py_string( "DELETE" );
					PyDict_SetItemString( t, "Action", s );
					Py_DECREF( s );
				} else if( (items[i].type( ) & SET) ) {
					if( f <= 0 ) {
						Py_DECREF( t );
						continue;
					}
					if( items[i].action( ) != REPLACE ) {
						PyObject *a = py_string( items[i].action_string( ) );
						PyDict_SetItemString( t, "Action", a );
						Py_DECREF( a );
					}
					
					PyObject *v = PyList_New( (Py_ssize_t) f );
					for( size_t j = 0; j < f; ++ j ) {
						const string_t &s = items[i]._list( )[j];
						if( s.size( ) > 0 ) {
							PyList_SET_ITEM( v, j, py_string( s ) );
						}
					}
					PyObject *o = PyDict_New( );
					PyDict_SetItemString( o, items[i].type_string( ), v );
					Py_DECREF( v );
					PyDict_SetItemString( t, "Value", o );
					Py_DECREF( o );
				} else {
					if( items[i].type( ) == NUMBER && items[i].action( ) == ADD ) {
						PyObject *a = py_string( "ADD" );
						PyDict_SetItemString( t, "Action", a );
						Py_DECREF( a );
					}
					PyObject *o = PyDict_New( );
					PyObject *s = py_string( items[i]._value( ) );
					PyDict_SetItemString( o, items[i].type_string( ), s );
					Py_DECREF( s );
					PyDict_SetItemString( t, "Value", o );
					Py_DECREF( o );
				}
				PyObject *key = py_string( items[i].name( ) );
				PyDict_SetItem( r, key, t );
				Py_DECREF( key );
				Py_DECREF( t );
			}
			if( unlikely( py_error( "dict_from_items_update" ) ) ) {
				py_release( r );
				return NULL;
			}
			return r;
		}
		
		static PyObject *list_from_items( const item_list_t &items ) _noexcept {
			const size_t e = items.size( );
			PyObject *r = PyList_New( (Py_ssize_t) e );
			for( size_t i = 0; i < e; ++ i ) {
				PyObject *key = py_string( items[i].name( ) );
				PyList_SET_ITEM( r, i, key );
//				Py_DECREF( key );
			}
			if( unlikely( py_error( "list_from_items" ) ) ) {
				py_release( r );
				return NULL;
			}
			return r;
		}
		
		static inline bool item_from_dict( PyObject *obj, item &output ) _noexcept {
			if( obj == NULL ) {
				return false;
			}
			// exactly 1 attribute, but with unknown key, so just fetch an attribute at random
			PyObject *key; // borrowed
			PyObject *value; // borrowed
			Py_ssize_t pos = 0;
			if( !PyDict_Next( obj, &pos, &key, &value ) ) {
				fprintf( stderr, "malformed record (no data)\n" );
				return false;
			}
			
			if( unlikely( !output.set_type( type_from_string( py_cstring( key ) ) ) ) ) {
				return false;
			}
			if( unlikely( output.type( ) == UNKNOWN ) ) {
				fprintf( stderr, "malformed record (unknown type)\n" );
				return false;
			}
			if( (output.type( ) & SET) ) {
				output.clear_items( );
				
				const size_t l = (size_t) PyList_Size( value );
				for( size_t i = 0; i < l; ++ i ) {
					if( unlikely( !output.add_item( py_cstring( PyList_GET_ITEM( value, i ) ) ) ) ) {
						output.clear_items( );
						return false;
					}
				}
			} else {
				const char *v = py_cstring( value );
				if( v == NULL ) {
					fprintf( stderr, "malformed record (value is not a string)\n" );
					return false;
				}
				if( unlikely( !output.set_value( v ) ) ) {
					return false;
				}
			}
			output.set_action( REPLACE );
			return true;
		}
		
		static bool update_from_dict( item_list_t &items, PyObject *ret_items ) _noexcept {
			if( items.size( ) == 0 ) {
				PyObject *key; // borrowed
				PyObject *itm; // borrowed
				Py_ssize_t i = 0;
				while( PyDict_Next( ret_items, &i, &key, &itm ) ) {
					item o( py_cstring( key ) );
					if( unlikely( !item_from_dict( itm, o ) ) ) {
						continue;
					}
					try {
						items.push_back( o );
					} catch( ... ) {
						return false;
					}
				}
			} else {
				for( size_t i = items.size( ); (i --) > 0; ) {
					PyObject *key = py_string( items[i].name( ) );
					PyObject *itm = PyDict_GetItem( ret_items, key ); // borrowed
					Py_DECREF( key );
					
					if( !item_from_dict( itm, items[i] ) ) {
						items.erase( items.begin( ) + (std::ptrdiff_t) i );
						continue;
					}
				}
			}
			return true;
		}
		
		static bool update( const const_string_t &db, const const_string_t &key, const item_list_t &items, const item_list_t *expected ) _noexcept {
			/* http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_UpdateItem.html
			 * ret = layer1.update_item( [table],
			 *   {'HashKeyElement':{'S':[key]}},
			 *   {
			 *     [attr1]:{'Value':{[T]:[value]}},
			 *     [attr2]:{'Value':{[TS]:[[value1],[value2]]},'Action':'ADD'}
			 *   }
			 * )
			 * used = ret.ConsumedCapacityUnits
			 */
			
			PyObject *layer1 = prep( );
			if( unlikely( layer1 == NULL ) ) {
				return false;
			}
			
			PyObject *key_dict = PyDict_New( );
			PyObject *key_str = py_string( key );
			PyObject *key_prop = PyDict_New( );
			PyDict_SetItemString( key_prop, "S", key_str );
			Py_DECREF( key_str );
			PyDict_SetItemString( key_dict, "HashKeyElement", key_prop );
			Py_DECREF( key_prop );
			
			PyObject *expect = NULL;
			if( expected != NULL ) {
				if( expected->size( ) > 0 ) {
					expect = dict_from_items_expect( *expected );
				}
			}
			
			PyObject *ret = py_callfunc( layer1, "update_item",
				"", py_string( db ),
				"", key_dict,
				"", dict_from_items_update( items ),
				(expect != NULL) ? "expected" : "-", expect,
			NULL );
			
			if( unlikely( ret == NULL ) ) {
				return false;
			}
			
			PyObject *cap = PyDict_GetItemString( ret, "ConsumedCapacityUnits" ); // borrowed
			if( unlikely( cap == NULL ) ) {
				fprintf( stderr, "bad response when saving record in table \"%.*s\"\n", SIZED_STRING(db) );
				Py_DECREF( ret );
				return false;
			}
			fprintf( stderr, "saved record, used %f capacity units\n", PyFloat_AsDouble( cap ) );
			Py_DECREF( ret );
			return true;
		}
		
		static bool get( const const_string_t &db, const const_string_t &key, bool consistent, item_list_t &items ) _noexcept {
			/* http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_GetItem.html
			 * layer1.get_item( [database_name], {'HashKeyElement':{'S':[key]}} )
			 */
			
			PyObject *layer1 = prep( );
			if( unlikely( layer1 == NULL ) ) {
				return false;
			}
			
			PyObject *key_dict = PyDict_New( );
			PyObject *key_str = py_string( key );
			PyObject *key_prop = PyDict_New( );
			PyDict_SetItemString( key_prop, "S", key_str );
			Py_DECREF( key_str );
			PyDict_SetItemString( key_dict, "HashKeyElement", key_prop );
			Py_DECREF( key_prop );
			
			PyObject *ret = py_callfunc( layer1, "get_item",
				"", py_string( db ),
				"", key_dict,
				"attributes_to_get", list_from_items( items ),
				"consistent_read", Py_INCREF( consistent ? Py_True : Py_False ),
			NULL );
			
			if( unlikely( ret == NULL ) ) {
				return false;
			}
			
			PyObject *cap = PyDict_GetItemString( ret, "ConsumedCapacityUnits" ); // borrowed
			PyObject *ret_items = PyDict_GetItemString( ret, "Item" ); // borrowed
			if( unlikely( ret_items == NULL ) ) {
				fprintf( stderr, "failed to load record items from table \"%.*s\"\n", SIZED_STRING(db) );
				Py_DECREF( ret );
				return false;
			}
			if( unlikely( cap == NULL ) ) {
				fprintf( stderr, "loaded record, but capacity units used is unknown\n" );
			} else {
				fprintf( stderr, "loaded record, used %f capacity units\n", PyFloat_AsDouble( cap ) );
			}
			
			if( unlikely( !update_from_dict( items, ret_items ) ) ) {
				fprintf( stderr, "out of memory when loading items from table \"%.*s\"\n", SIZED_STRING(db) );
				Py_DECREF( ret );
				return false;
			}
			Py_DECREF( ret );
			return true;
		}
		
		static inline void disconnect( void ) _noexcept {
			(void) prep( true );
		}
	}
}

#endif
