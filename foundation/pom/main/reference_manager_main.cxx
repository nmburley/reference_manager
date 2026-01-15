// @<COPYRIGHT>@
// ================================================
// Copyright 2017.
// Siemens Product Lifecycle Management Software Inc.
// All Rights Reserved.
// ================================================
// @<COPYRIGHT>@

/**
    @file

 *: This program is used to manage typed and untyped references 
 *: along with other helpful diagnostic functions.
 *:
 *: See the dspUsage() routine for all supported usages of this utility.
 *:
 *: General command format is:
 *:
 *: reference_manager  <utility_option> -u=username -p=password | -pf=pwdfile -g=group <option-paramters> ...
*/

/*
** The following two defines allow us to keep all of the reference_manager code in a single location.
** This will be advantageous when we add additional functionality as we can test Tc10 functionality before
** backporting to TC10 and the diffs for backporting will be extremely simple.
**
** There are several different modes that the reference_manager can be compiled for.  They are as follows.
**
** Compiled for Tc13.3 and later:                                PRE_TC133_PLATFORM is NOT defined.
** Compiled for Tc13.1 and later:                                PRE_TC131_PLATFORM is NOT defined.
** Compiled for Tc11 and ealier                                  PRE_TC12_PLATFORM  is     defined
** Compiled for Tc11 with Tc11 reference manager functionality:  PRE_TC11_PLATFORM  is NOT defined  AND  PRE_TC11_REF_MGR is NOT defined
** Compiled for Tc11 with Tc10 reference manager functionality:  PRE_TC11_PLATFORM  is NOT defined  AND  PRE_TC11_REF_MGR is     defined
** Compiled for Tc10 with Tc10 reference manager functionality:  PRE_TC11_PLATFORM  is     defined  AND  PRE_TC11_REF_MGR is     defined
*/
// #define PRE_TC133_PLATFORM
// #define PRE_TC131_PLATFORM
// #define PRE_TC12_PLATFORM
// #define PRE_TC11_REF_MGR
// #define PRE_TC11_PLATFORM

// #define PRE_TC11_2_3_PLATFORM
// #define LOCAL_CACHE_CPIDS

#define REF_MGR_VERSION  "April 2023"

#if defined(UNX)
#include <extended_source.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unidefs.h>
#include <utility>
#include <om.h>
#include <om_privileged.h>
#include <sm.h>
#include <error.h>
#include <sys_syss.h>
#include <arg.h>
#include <stdlib.h>
#include <ctime>
#include <sstream>
#include <base/PasswordFile.h>
#include <base_utils/Format.hxx>
#include <pom/pom/om_flags.hxx>
#include <pom/dds/dds.h>
#include <pom/dds/dds_internal.h>
#include <pom/dms/dms.h>
#if !defined(PRE_TC12_PLATFORM)
#include <pom/dms/dms_class_storage_mode.hxx>
#endif
#include <pom/eim/eim.h>
#include <pom/eim/eim_privileged.h>
#include <pom/eim/eim_sqlc.h>
#include <pom/enq/enq.h>
#include <pom/enq/enq_privileged.h>
#include <pom/enq/pom_enquiry_internal.h>
#include <pom/fss/fss.hxx>
#include <pom/pom/om_flags.hxx>
#include <pom/pom/pom.h>
#include <pom/pom/pom_dd.h>
#include <pom/pom/pom_env_vars.hxx>
#include <pom/pom/pom_privileged.h>
#include <pom/pom/pom_mapping.h>
#include <pom/pom/pom_tokens.h>
#include <pom/pom/om_flags.hxx>

#include <pom/ril/ril_applic.h>
#include <pom/sss/sss.h>
#include <str.h>

#include <om_meta_structures.h>
#include <mld/logging/Logger.hxx>
#include <pom/pom/POMLogger.hxx>

#ifndef PRE_TC11_PLATFORM
#ifndef PRE_TC131_PLATFORM
#include <pom/pom/pom_utility_transaction.hxx>
#include <base_utils/SystemUtils.h>
#else
#include <pom/pom/pom_external_transaction.hxx>
#include <base_utils/SystemUtils.h>
#endif
#endif

#ifdef PRE_TC11_2_3_PLATFORM
#include <base_utils/SystemUtils.h>
#endif

#include <base_utils/OSEnvironment.hxx>
#include <TcCrypto/TcCrypto.h>

#ifdef WNT
#define strcasecmp stricmp
#define strncasecmp strnicmp
#endif

#ifndef PRE_TC11_REF_MGR
#ifndef PRE_TC131_PLATFORM
#define START_WORKING_TX( name__, id__ ) \
    start_working_tx( id__ ); \
    Teamcenter::POMUtilityTransaction name__( id__ )
#else
#define START_WORKING_TX( name__, id__ ) \
    start_working_tx( id__ ); \
    Teamcenter::POMExternalTransaction name__( id__ )
#endif

#define COMMIT_WORKING_TX( name__, id__ ) \
    name__.commit( id__ ); \
    commit_working_tx( )

#define ROLLBACK_WORKING_TX( name__, id__ ) \
    name__.rollback( 0 , id__ ); \
    rollback_working_tx( )
#else
#define START_WORKING_TX( name__, id__ ) \
    start_working_tx( id__ ); 

#define COMMIT_WORKING_TX( name__, id__ ) \
    commit_working_tx( )

#define ROLLBACK_WORKING_TX( name__, id__ ) \
    rollback_working_tx( )
#endif


#define OK 0
#undef FAIL
#define FAIL -1

#define FLATTENED_CLASS_PROPERTY OM_class_prop_has_flat_tables
#define CLS_NAME_SIZE    33
#define CLS_DB_NAME_SIZE 33
#define ATT_NAME_SIZE    33
#define ATT_DB_NAME_SIZE 33
#define MAX_UID_SIZE     15
#define HAS_FLAT_SUBCLASSES 1
#define IS_FLAT_CLASS       2

#define MAX_COUNT_CHAR_SIZE 50

enum Op { none, find_ref, find_ext_ref, check_ref, find_class, find_stub, load_obj, add_ref, remove_ref, validate_bp, correct_bp, delete_obj, where_ref, str_len_val, str_len_meta, scan_vla, remove_unneeded_bp, validate_bp2, edit_array, validate_cids };
enum Meta_Type { ref, ext_ref, vla };

/* Structure to hold the command line argument and other useful info*/
typedef  struct args
{
    char *root_exe;          /**< Root name of the executable */
    Op   op;                 /**< Current operation */
    char *username;          /**< Username */
    int  user_flag ;         /**< Username has been specified */
    char *password;          /**< User's password */
    int  pwd_flag ;          /**< User's password has been specified */
    char *pwf;               /**< File containing password */ 
    int  pwf_flag;           /**< Password file has been specfied */
    char *usergroup;         /**< User's group */
    int  group_flag;         /**< User's group has been specified */
    char *uid;               /**< Object ID */
    std::vector< std::string > *uid_vec; /**< Set of Object IDs */
    int  uid_flag;           /**< Object ID has been specified */
    char *class_n;           /**< Class name */
    int  class_flag;         /**< Class name has been specified */
    char *attribute;         /**< Class attribute */
    int  attribute_flag;     /**< Class attribute has been specified */
    char *not_supported;     /**< Unsupported option */
    int  not_supported_flag; /**< Unsupported option has been specified */
    int  debug_flag;         /**< Debug parameter has been specified */
    int  verbose_flag;       /**< Verbose parameter has been specified */
    int  noparallel_flag;    /**< Disable parallel parameter has been specified */
    int  ignore_errors_flag; /**< Ignore errors parameter has been specified */
    char *class_obj_n;       /**< An object's class name */
    int  class_obj_flag;     /**< An object's class name has been specified */
    int  att_cnt;            /**< Number of attributes to be processed */
    int  max_ref_cnt;        /**< Maximum number of references to process */
    int  force_flag;         /**< Attempt to force load the object using the specified class name (class_n) */
    char *from;              /**< The referencing object in a reference */
    int  from_flag;          /**< The referencing object has been specified */
    char *to;                /**< The referenced object in a reference */
    int  to_flag;            /**< The referenced object has been specified */
    int  deleted_flag;       /**< User is asserting that the object has been deleted */
    int  null_ref_flag;      /**< Parameter to specify that a null-reference is preferred over a null-value */
    int  commit_flag;        /**< Operation is to be commited to the database */
    int  all_flag;           /**< User has requested that all objects or references be processed */
    int  ext_ref_flag;       /**< User has specified that we are working with external references */
    int  expected_error;     /**< Expected AOS error - This error values is converted to zero if detected */
    char *lic_key;           /**< Licensing key specified on the command line */    
    int  lic_key_flag;       /**< Flag indicating that a licensing key was specified on the command line. */
    char *lic_file;          /**< Licensing file containing licensing key */
    int  lic_file_flag;      /**< Flag indicating that a licensing file was specified on the command line. */
    int  where_ref_sub;      /**< Where reference sub_option, 1=ImanRelation + POM_BACKPOINTER, 2 = all_backpointer_references view */
    int  min_flag;           /**< Process the option with a minumal amount of functionality */
    int target_cnt;          /**< The target UID count. If the operation does not find this many UIDs then an error is genearted. */
    int help;                /**< 0 = minimum usage help, 1 = detailed help information. */
    int keep_system_log;     /**< true = keep the system log when process has terminated */
    logical alt;             /**< true = keep the system log when process has terminated */
    char* file_name;         /**< File name from the command line */
    logical log_details;     /**< true = log addition details in the syslog and keep the system log when process has terminated */
    char* val_class_n;       /**< Validation class name, primarily used for the definitive source of object details such as the object's class ID (cpid), etc. */
 } args_t;


/* Structure to hold backpointer information info*/
typedef  struct bp
{
    char  from_uid[MAX_UID_SIZE+1];           /**< Backpointer from UID */
    int   from_class;                         /**< Backpointer from class ID */
    char  from_class_name[CLS_NAME_SIZE+1];   /**< Backpointer from class name */

    char  to_uid[MAX_UID_SIZE+1];             /**< Backpointer to UID */
    int   to_class;                           /**< Backpointer to class ID */
    char  to_class_name[CLS_NAME_SIZE+1];     /**< Backpointer to class name */

    int   bp_count;                           /**< Backpointer reference count */
 } bp_t;


/* Structure to store referencing objects. */
typedef struct ref
{
    char uid[MAX_UID_SIZE + 1];             /**< Object IDs (UIDs) */
 } ref_t;


/* Structure to store references with bad class IDs */
typedef struct ref_cpid
{
    char uid[MAX_UID_SIZE + 1];              /**< Object IDs (UIDs) */
    int ref_cid;                             /**< class ID found in the reference (bad class ID) */
    int tar_cid;                             /**< class ID found in the target table (good class ID) */
} ref_cpid_t;


/* Structure to store attribute information. */
typedef struct att
{
    char   name[ATT_NAME_SIZE+1];            /**< Attribute's name */
    char   db_name[ATT_DB_NAME_SIZE+1];      /**< Attribute's database name */
    int    att_id;                           /**< Attribute's ID */
    int    plength;                          /**< Attribute's array length */
    int    pproperties;                      /**< Attribute's properties */
    int    ptype;                            /**< Attribute's type */
    int    pptype;                           /**< Attribute's type */
    int    ref_cpid;                         /**< Attribute's reference type - for typed references */
    int    uid_cnt;                          /**< Attribute's found referencing UID count - only temporary until output to console */
    ref_t* uids;                             /**< Attribute's found referencing UIDs -- only temporary until output to console */
 } att_t;


/* Structure to store class information. */
typedef  struct cls
{
    char   name[CLS_NAME_SIZE+1];            /**< Class name */
    char   db_name[CLS_DB_NAME_SIZE+1];      /**< Class database name */
    int    cls_id;                           /**< Class ID */
    int    properties;                       /**< Class properties */
    int    ref_cnt;                          /**< Number of references found in class tables */
    int    flt_cnt;                          /**< Number of references found in flat tables */
    int    att_cnt;                          /**< Attribute count */
    att_t* atts;                             /**< Class attributes */
 } cls_t;


/* Structure to store class hierarchy. */
typedef  struct hierarchy
{
    int     cls_id;                          /**< Class ID */
    int     par_id;                          /**< Parent ID */
    int     refs;                            /**< Number of reference attributes */
    int     cls_pos;                         /**< Position of class within metadata structure */
    int     flags;                           /**< 0x1 = has flattened subclasses, 0x2 = is flattened class */
 } hier_t;

  /* Structure used to optimize (eliminate) searching for stubbed objects. */
 typedef struct minny_meta
 {
     int cls_id;                            /**< Class ID */
     char name[CLS_NAME_SIZE + 1];          /**< Class name */
     char db_name[CLS_DB_NAME_SIZE + 1];    /**< Class table name  */
     int cls_pos;                           /**< Position of class within metadata structure */
     int count;                             /**< Varies by context. Ex. number of stubs */
 } minny_meta_t;

 // Defines to simulate metadata for tables such as POM_BACKPOINTER */
#define TBL_META_PARENT_CID       0
#define TBL_META_CLASS_FLAGS      0
#define TBL_META_CLASS_PROPERTIES 0
 // DDS_type_untyped_ref = 115
#define COL_META_UT_REF_PPTYPE    DDS_type_untyped_ref
#define COL_META_UT_REF_PTYPE     9
#define COL_META_PROPERTIES       0
 // The following identifies the attribute as a column on a table. 
#define COL_META_LENGTH           -2


/* function prototype */
static void cons_out( const std::string msg );
static void cons_out_no_log( const std::string msg );
static int error_out( const char *file_name, int line_number, int failure_code, const std::string msg );
static const char* find_root_file( const char *file_name );
static int getCmdLineArgs( int argc, char** argv, args_t *args );
static int get_backpointers( const std::string from_uid, const std::string to_uid, std::vector<bp_t> &bptrs );
static int report_backpointers( std::vector<bp_t> &bptrs );
static int report_object_refs( const std::string from_uid, int from_class, const std::string from_class_name, const std::string to_uid, int to_class, const std::string to_class_name, int ref_count, const std::string cmd_from_class, const std::string cmd_to_class );
// static std::string get_class_name( int cpid );
static int get_cpid( const std::string class_name );
static int adjust_backpointers( const std::string from_uid, int from_cpid, const std::string to_uid, int to_cpid, int bp_count );
static int isValidOptionArgument( const args_t& args );
static int isPotentialPasswordAvailable( args_t& args );
static char* findExeRoot( char* exe );
static void dspUsage( const char* exe );
static int getMetadata( std::vector<cls_t>& meta, std::vector<hier_t>& hier, Meta_Type m_type );
static int getExtRefMeta( std::vector< cls_t > &meta, std::vector< hier_t > &hier );
static int getRefMeta( std::vector< cls_t > &meta, std::vector< hier_t > &hier );
static int getVlaMeta( std::vector<cls_t>& meta, std::vector<hier_t>& hier );
static void filterOutFlatClasses( std::vector<cls_t>& meta, std::vector<minny_meta_t>& flat_classes );
static int dumpRefMetadata( const std::vector< cls_t > &meta );
static int getHierarchy( std::vector< hier_t > &hier );
static int dumpHierarchy( std::vector< hier_t > &hier );
static logical isScalar( const att_t *att );
static logical isVLA( const att_t *att );
static logical isLA( const att_t *att );
static logical isSA( const att_t *att );
static logical isFlat( const cls_t *cls );
static logical isColumn(const att_t* att);
static void output_att_err( const cls_t *cls, const cls_t *flat, const att_t *att, int ifail, const char* msg );
static void output_att_msg( const cls_t *cls, const cls_t *flat, const att_t *att, const char* msg );
static void output_att_data( const char prefix, const cls_t *cls, const cls_t *flat, const att_t *att );
static void output_att_ref_cid_data( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att );
static std::string get_ref_table( const cls_t *cls, const cls_t *flat, const att_t *att );
static std::string get_ref_column( const att_t* att, int sa_offset, bool for_dsp = false );
static std::string get_ref_col_where_expr( const att_t *att, int sa_offset, const std::string value );
static std::string get_ref_table_and_column( const cls_t *cls, const cls_t *flat, const att_t *att, int sa_offset, bool for_dsp = false );
static std::string get_vla_class_table( const cls_t* cls, const cls_t* flat );
static std::string get_vla_table( const att_t* att );
static std::string get_vla_count_column( const cls_t* cls, const att_t* att );
static std::string get_vla_seq_column( const att_t* att );
static void output_scan_vla_err( const cls_t* cls, const cls_t* flat, const att_t* att, int ifail, const char* msg );
static void output_scan_vla_msg( const cls_t* cls, const cls_t* flat, const att_t* att, const char* msg );
static void output_scan_vla_attr_hdr( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att, int uid_cnt );
static void output_scan_vla_details( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att );
static void output_scan_vla_parallel_data( const cls_t* cls, const cls_t* flat, att_t* attr );
static int get_inconsistent_vlas( cls_t* cls, const cls_t* flat, att_t* att );
static int output_vlas( cls_t* cls, int* accum_cnt, int* attr_cnt, char** last_attr_name );
static int output_flattened_vlas( const std::vector<hier_t>& hier, std::vector<cls_t>& meta, cls_t* flat, int* accum_cnt, int* attr_cnt );
static std::string get_sa_where_clause( const att_t *att );
static std::string get_sa_sql_extension( const att_t *att, const std::string base_sql, const std::string to_uid );
static int get_refs( cls_t *cls, const cls_t *flat, att_t *att );
static int output_refs( cls_t *cls, int *accum_cnt, int *attr_cnt, char **last_attr_name );
static int output_flattened_refs( const std::vector<hier_t>& hier, std::vector<cls_t>& meta, cls_t* flat, int* accum_cnt, int* attr_cnt );
static int get_ref_cnt( const cls_t *cls, const cls_t *flat, const att_t *att, const std::string from_uid, const std::string to_uid, int* count );
static int find_ref_count( int from_cpid, const std::string from_uid, const std::string to_uid, int to_cpid, int* ref_count );
static int query_class_to_find_class_of_uid ( std::vector< std::string > uids, const std::string target_class, std::map< std::string, std::string > &found_classes );
static int query_class_to_find_class_of_uid (  const std::string uid, const std::string target_class, std::string  &found_class );
static std::string get_class_name_from_cpid( std::string cpid );
static int get_flattened_class_names( std::vector< std::string > &flattened_classes );
static int find_unresolved( std::vector< std::string > &original_vec, std::map< std::string, std::string > &result_map, std::vector< std::string > &unresolved_vec );
static int find_unresolved( const std::string class_name, std::vector< std::string > &original_vec, std::map< std::string, std::string > &result_map, std::vector< std::string > &unresolved_vec );
static int get_and_validate_class_tag(const std::string uid, const std::string target_class, std::string &found_class, tag_t* classTag, logical* db_validated, const char* option);
static int lockAndUnlock(tag_t objTag, tag_t classTag, const std::string uid);
static int validate_cmd_line_class_and_attribute_params( );
static std::string bitwise_and( const char* column, int value );
static void output_stub_details( std::vector<std::string>* uid_vec, int* found_count );

static logical refreshToLock( tag_t tag, int pom_lock, tag_t class_tag );
static logical loadToNoLock( tag_t tag, tag_t class_tag );
static logical unload( tag_t tag );
static std::string getSubParameter( const char* param, int sub_param_pos );

static int start_working_tx( const char* tx_id );
static int commit_working_tx( );
static int rollback_working_tx( );
static int database_tx_check();

static int find_ref_op( int *found_count );
static int find_ext_ref_op();
static int check_ref_op();
static int find_class_op();
static int find_stub_op( int* found_count );
static int load_obj_op();
static int add_ref_op();
static int remove_ref_op();
static int delete_obj_op();
static int validate_bp_op( Op op );
static int correct_bp_op( Op op );
static int validate_or_correct_bp( Op op );
static int where_ref_op();
static int str_len_val_op(int* bad_count );
static int str_len_meta_op();
static int scan_vla_op( int* found_count ); 
static int remove_unneeded_bp_op( int* found_count );
static int validate_bp2_op( int* found_count );
static int validate_cids_op( int* found_count );
static int edit_array_op( );

static logical compare_object_to_bp( const std::string from_uid, const std::string from_class, const std::string to_uid, const std::string to_class, int refs, std::vector< bp_t > &bptrs );
static logical is_digit( char ch );

static int get_string_sizes(const char *cls_tbl, const char *cls_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &calc_sizes);
static int validate_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &calc_sizes, int* bad_count);
static int truncate_strings(const char *tbl, const char *col, int max_size, EIM_uid_t *uids, int* ints, int size);

static int get_vla_string_sizes(const char *cls_tbl, const char *cls_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes);
static int validate_vla_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes, int* bad_count);
static int truncate_vla_strings(const char *tbl, const char *col, int max_size, EIM_uid_t *uids, int* seqs, int* ints, int size);

static int get_la_string_sizes(const char *att_tbl, const char *att_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes);
static int validate_la_string_sizes(const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes, int* bad_count);

static int get_long_string_sizes(const char *cls_tbl, const char *cls_col, const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes);
static int validate_long_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &calc_sizes, int* bad_count);
static int bulk_update_integers(const char *tbl, const char *col, EIM_uid_t *uids, int* ints, int size);

static int get_vla_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes);
static int validate_vla_long_string_sizes(const char* func, const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count);
static int bulk_update_vla_integers(const char *tbl, const char *col, EIM_uid_t *uids, int* seqs, int* ints, int size);

static int get_la_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes);
static int validate_la_long_string_sizes(const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count);

static int get_sa_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes);
static int validate_sa_long_string_sizes(const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count);

static int get_tables_and_columns(const char *cls_name, const char* att_name, const char** cls_tbl, const char** cls_col, const char** att_tbl, const char** att_col);
static int get_string_meta(const char* cls, const char** super);

static int get_class_table_name( const char* cls_name, const char** cls_tbl );
static std::string get_storage_mode( const char* cls_name );
static std::string get_top_query_table( int cid );

/* **********************
** Licensing routines
** *********************/
static void           licensing_start();
static void           licensing_stop();
static logical        setKey( const std::vector<unsigned char> key );
static unsigned char* BASE_decrypt( const unsigned char* base64CryptText );
static void           fromBase64( const unsigned char* base64String, unsigned char** cryptText, unsigned int* cryptTextLen );
static std::vector<unsigned char> readStringFromFile( const char* fName, bool silent );
static const void     PrintErrors( const char *file, const int line, const char *method, const Teamcenter::Logging::Logger::level lvl );
static int            licenseFor( const char* option );

/** Filename for alternate encryption key.  If this file exists in TC_DATA and has reasonable contents,
    use it instead of the default encryption key. */
static const char _alternateKeyFile[] =
#if defined( WNT )
"\\UtilLicKey";
#else
"/UtilLicKey";
#endif // defined( WNT )

/** Default encryption key */
static const std::string _defaultKey( "9ywe34cNqds2PsF9LnPDv5bs6d5TbYga" );

/** Static Base64 character encoding lookup table */
static const char _base64EncodeTable[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/** Static Base64 character decoding lookup table.
    All the -1 entries ensure that any character outside the Base64 character set is mapped to 0xFF */
static const char _base64DecodeTable[256] = {
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
    ,-1,62,-1,-1,-1,63,52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    ,22,23,24,25,-1,-1,-1,-1,-1,-1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
    ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1};

static TcCryptoContext* _context = NULL;
static unsigned char _secretKey[ MAX_KEY_LENGTH ];
static logical _isInitialized = false;
static unsigned char* _iv      = NULL;  /* Possibly _iv and _ivLength should not */
static unsigned int _ivLength  = 0;     /* even be data members */
static Teamcenter::Logging::Logger* _logger = NULL;

// Maximum plausible input line lengths for input
// strings and Base64 encoded strings.  Values were arbitrarily chosen.
static const unsigned int MAX_INPUT_LENGTH = 4096;
static const unsigned int MAX_BASE64_LENGTH = 8192;
/* *********************/


#ifdef PRE_TC11_PLATFORM
#define POM_delete_lock POM_modify_lock
#define fnd_printf printf
#endif

#if defined(PRE_TC11_PLATFORM) || defined(LOCAL_CACHE_CPIDS)
static int DMS_cache_cpids_of_class( const unsigned int n_tags, const tag_t *instances, const char *class_name, unsigned int *prior_reg, unsigned int *final_reg );
#endif


static args_t *args;
static int DDS_array_value_g = 7;  /* Arrays of length 7 or greater are large arrays */


/* Each image target has its own unique logger named after itself. */
static Teamcenter::Logging::Logger* logger()
{
    static Teamcenter::Logging::Logger* pLogger = 0;
    if ( !pLogger )
    {
        pLogger = Teamcenter::Logging::Logger::getLogger( "Teamcenter.POM.reference_manager_main" );
    }
    return pLogger;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
extern int main (int argc, char **argv)
{
    try
    {
        int ifail;
        tag_t auser,theWorld;
        int version;

        SM_init_module();
        STR_init_module();

        // The following code is borrowed from the install utility.
        // Horrid hacky, but we rename ourselves by concatenating the operation name
        // with the program name before we call ARG_init_module.  This way the SYSS
        // program name gets set to something more interesting than "reference_manager" so the
        // syslog file gets an interesting name
#define PROGRAM_NAME_LENGTH 500
#define OPERATION_NAME_LENGTH 500
        char* backup_program_name = argv[0];
        if ( argv[0] != 0 && strlen( argv[0] ) < PROGRAM_NAME_LENGTH &&
             argv[1] != 0 && strlen( argv[1] ) < OPERATION_NAME_LENGTH )
        {
            static char hacks_r_us[PROGRAM_NAME_LENGTH + OPERATION_NAME_LENGTH + 1];
            sprintf( hacks_r_us, "%s%s", argv[0], argv[1] );
            argv[0] = hacks_r_us;
        }
        ARG_init_module( argc, argv );
        argv[0] = backup_program_name;
        SSS_create_tc_syslog();
        if ( SSS_getenv( "TC_KEEP_SYSTEM_LOG" ) != 0 )
        {
            ERROR_set_log_file_status( ERROR_KEEP_LOG_FILE );
        }

        /* can't use SM yet as it's not initialized until below */
        args = static_cast< args_t*>(malloc( sizeof(args_t) ));
        args->root_exe = findExeRoot( argv[0] );
        args->op = none;
        args->user_flag = FALSE;
        args->pwd_flag = FALSE;
        args->pwf_flag = FALSE;
        args->username = 0;
        args->password = 0;
        args->pwf = 0;           // after isPotentialPasswordAvailable() is called args->pwf contains an sm_allocated password.
        args->usergroup = 0;
        args->group_flag = FALSE;
        args->uid = NULL;
        args->uid_vec = new std::vector< std::string >();
        args->uid_flag = FALSE;        
        args->class_n = NULL;
        args->class_flag = FALSE;        
        args->attribute = NULL;
        args->attribute_flag = FALSE;
        args->not_supported = NULL;
        args->not_supported_flag = FALSE;
        args->verbose_flag = FALSE;
        args->debug_flag = FALSE;
        args->noparallel_flag = FALSE;
        args->ignore_errors_flag = FALSE;
        args->class_obj_flag = FALSE;
        args->class_obj_n = NULL;
        args->att_cnt = 0;
        args->max_ref_cnt = 100;
        args->force_flag = FALSE;
        args->to_flag = FALSE;
        args->from_flag = FALSE;
        args->deleted_flag = FALSE;
        args->null_ref_flag = FALSE;
        args->commit_flag = FALSE;
        args->all_flag = FALSE;
        args->ext_ref_flag = FALSE;
        args->expected_error = 0;
        args->lic_key = NULL;
        args->lic_key_flag = FALSE;
        args->lic_file = NULL;
        args->lic_file_flag = FALSE;
        args->where_ref_sub = 0;
        args->min_flag = FALSE;
        args->target_cnt = -1;
        args->help = 0;
        args->keep_system_log = FALSE;
        args->alt = FALSE;
        args->file_name = NULL;
        args->log_details = FALSE;
        args->val_class_n = NULL;

        getCmdLineArgs( argc, argv, args );

        if ( args->help > 0 )
        {
            dspUsage( findExeRoot( argv[0] ) );
            return (OK);
        }

        if ( args->keep_system_log || args->log_details )
        {
            ERROR_set_log_file_status( ERROR_KEEP_LOG_FILE );
        }

        if(isValidOptionArgument(*args) == FAIL || isPotentialPasswordAvailable(*args) == FAIL || argc<5 || args->op == none)
        {
            if( args->not_supported_flag )
            {
                std::stringstream msg;
                msg << "ERROR: Invalid option (" << args->not_supported << ") was specified on the command line";
                cons_out( msg.str() );
            }

            if( args->not_supported_flag == none )
            {
                std::stringstream msg;
                msg << "ERROR: Missing reference manager operation";
                cons_out( msg.str() );
            }

            dspUsage( findExeRoot( argv[0]) );
            return (FAIL);
        }

        if( args->lic_key_flag == TRUE && args->lic_file_flag == TRUE )
        {
            const std::string msg( "Both the -lic_key option and the -lic_file option can't be specified.");
            cons_out( msg );
            dspUsage( findExeRoot( argv[0]) );
            return (FAIL);
        }

        /* Initialize Foundation System Services */

        if (FSS_init() != 0)
        {
            const std::string msg( "Failed to initialize Foundation System Services. Exiting...");
            cons_out( msg );
            exit(EXIT_FAILURE);
        }

        if (!(EIM_dbplat()==EIM_dbplat_oracle || EIM_dbplat()==EIM_dbplat_mssql || EIM_dbplat()==EIM_dbplat_postgres))
        {
            const std::string msg( "Unsupported database platform. Exiting..." );
            cons_out( msg );
            return 0;
        }

        ifail=POM_start(args->username, args->pwf, args->usergroup,&auser,&theWorld,&version);

        memset(args->pwf, 0, strlen(args->pwf)); // zero out and free memory holding the password.
        SM_free(args->pwf);
        args->pwf = 0;

        if (ifail != POM_ok)
        {
            std::stringstream msg;
            msg << "Unable to log into the Teamcenter server. Please check your configuration: " << ifail;
            cons_out( msg.str() );
            exit(1);
        }

        int INFOMANAGERId, INFOMANAGERCode;

        if ( POM_register_application("INFOMANAGEV200", "INFOMANAGEV200", &INFOMANAGERId, &INFOMANAGERCode) == POM_ok)
        {
            if ((ifail = POM_identify_application(INFOMANAGERId, INFOMANAGERCode, true)) != POM_ok )
            {
                std::stringstream msg;
                msg << "POM failed (" << ifail << ") to register IMAN application";
                cons_out( msg.str() );
                exit(1);
            }
        }

        // If we are keeping the system log make sure we logged timed SQL.
        if ( args->keep_system_log )
        {           
            int dest = EIM_sqlca_debug_dest();
            if ( dest < EIM_debug_dest_syslog )
            {
                EIM_sqlca_set_debug_dest( EIM_debug_dest_syslog );
            }
            EIM_sqlca_set_debug_timing( true );
        }

        /* We may get an error raised about application protecting in here so bypass it */
        RIL_applic_protection( true );

        EIM_start_transaction();

        int found_count = 0;

        switch (args->op)
        {
        case find_ref:
            cons_out( "\nOperation: find references" );
            ifail = find_ref_op( &found_count );
            break;

        case find_ext_ref:
            cons_out( "\nOperation: find external references" );
            ifail = find_ext_ref_op();
            break;

        case check_ref:
            cons_out( "\nOperation: check a reference" );
            ifail = check_ref_op();
            break;

       case find_class:
            cons_out( "\nOperation: find class" );
            ifail = find_class_op();
            break;

       case find_stub:
           cons_out( "\nOperation: find stub" );
           ifail = find_stub_op( &found_count );
           break;

       case load_obj:
           cons_out( "\nOperation: load object" );
           ifail = load_obj_op();
           break;

       case add_ref:
           cons_out( "\nOperation: add reference" );
           ifail = add_ref_op();
           break;

       case delete_obj:
           cons_out( "\nOperation: delete object" );
           ifail = delete_obj_op();
           break;

       case remove_ref:
           cons_out( "\nOperation: remove reference" );
           ifail = remove_ref_op();
           break;

       case validate_bp:
           cons_out( "\nOperation: validate backpointer" );
           ifail = validate_bp_op( args->op );
           break;

       case correct_bp:
           cons_out( "\nOperation: correct backpointer" );
           ifail = correct_bp_op( args->op );
           break;

      case where_ref:
           cons_out( "\nOperation: where-referenced" );
           ifail = where_ref_op();
           break;

      case str_len_val:
          cons_out("\nOperation: string length validation");
          ifail = str_len_val_op( &found_count );
          break;

      case str_len_meta:
          cons_out("\nOperation: string meta data");
          ifail = str_len_meta_op();
          break;

      case scan_vla:
          cons_out( "\nOperation: scan VLA" );
          ifail = scan_vla_op( &found_count );
          break;

      case remove_unneeded_bp:
          cons_out( "\nOperation: remove unneeded backpointers" );
          ifail = remove_unneeded_bp_op( &found_count );
          break;

      case validate_bp2:
          cons_out( "\nOperation: validate backpointers" );
          ifail = validate_bp2_op( (args->target_cnt >= 0 ? &found_count : nullptr) );
          break;

      case edit_array:
          cons_out( "\nOperation: edit array" );
          ifail = edit_array_op( );
          break;   

      case validate_cids:
          cons_out( "\nOperation: validate reference class-IDs" );
          ifail = validate_cids_op( (args->target_cnt >= 0 ? &found_count : nullptr) );
          break;

      default:
           cons_out( "\nInvalid operation has been specified" );
           ifail = FAIL;
           break;
        }

        if ( args->target_cnt >= 0 && found_count != args->target_cnt )
        {
            ifail = POM_invalid_value;
            cons_out( "" );
            std::stringstream msg;
            msg << "ERROR: reference_manager found " << found_count << ", however it should have found " << args->target_cnt << " records (-cnt=" << args->target_cnt << ")";
            cons_out( msg.str() );
        }

        cons_out( "" );
        std::stringstream msg;
        msg << "Operation has completed - operation exit code = " << ifail; 
        cons_out( msg.str() );

        EIM_commit_transaction( "reference_manager" );

        /* Don't strictly speaking need to switch this off but it is good practice */
        RIL_applic_protection( false );

        if( args->expected_error != 0 )
        {
            if( ifail == args->expected_error )
            {
                cons_out( "" );
                std::stringstream msg;
                msg << "Return code \"" << ifail << "\" has been changed to \"0\" via the \"-aos=\" parameter"; 
                cons_out( msg.str() );
                ifail = 0;
            }
            else if( ifail == 0 )
            {
                ifail = args->expected_error;
                cons_out( "" );
                std::stringstream msg;
                msg << "Return code \"0\" has been changed to \"" << ifail << "\" via the \"-aos=\" parameter"; 
                cons_out( msg.str() );
            }
        }

        POM_stop (true);

        return ifail;
    }
    catch (const std::exception& ex)
    {
        std::stringstream msg;
        msg << "Fatal error: top level std::exception handler called, caught exception " << ex.what();
        cons_out( msg.str() );
        return EXIT_FAILURE;
    }
    catch (...)
    {
        const std::string msg( "Fatal error: top level catch-all exception handler called");
        cons_out( msg );
        return EXIT_FAILURE;
    }
}

static int check_ref_op()
{
    int op_fail = OK;
    int ifail = OK;
    std::string cmd_from_uid;
    std::string cmd_from_class;
    std::string cmd_to_uid;
    std::string cmd_to_class;
    std::string wrk_from_class;
    tag_t       wrk_from_class_tag = NULL_TAG;
    tag_t       wrk_from_obj_tag = NULL_TAG;
    int         wrk_from_cpid = -1;
    logical     wrk_from_validated = false;
    std::string wrk_to_class;
    tag_t       wrk_to_class_tag = NULL_TAG;
    tag_t       wrk_to_obj_tag = NULL_TAG;
    int         wrk_to_cpid = -1;
    logical     wrk_to_validated = false;
    int ref_count = 0;

    /* check from:uid, to:uid specified */
    if( !args->from_flag ) {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-check_ref option requires the \"-from=class:uid\" parameter " );
    }
    else {
        cmd_from_class = getSubParameter( args->from, 0 );
        if( cmd_from_class.empty() ) {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a class subparameter - E.g. \"-from=class:uid\"" );
        }
        cmd_from_uid = getSubParameter( args->from, 1);
        if( cmd_from_uid.empty() ) {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a uid subparameter - E.g. \"-from=class:uid\"" );
        }
    }
    if( !args->to_flag ) {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-check_ref option requires the \"-to=class:uid\" parameter " );
    }
    else {
        cmd_to_class = getSubParameter( args->to, 0 );
        if( cmd_to_class.empty() ) {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a class subparameter - E.g. \"-to=class:uid\"" );
        }
        cmd_to_uid = getSubParameter( args->to, 1);
        if( cmd_from_uid.empty() ) {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a uid subparameter - E.g. \"-to=class:uid\"" );
        }
    }
    if( op_fail != OK ) {
        return( op_fail );
    }

    /* Validate to/from class, to/from uid */
    ifail = POM_class_id_of_class( cmd_from_class.c_str(), &wrk_from_class_tag );
    if( ifail != OK ) {
        std::stringstream msg;
        msg << "The \"from\" class (" << cmd_from_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    ifail = POM_class_id_of_class( cmd_to_class.c_str(), &wrk_to_class_tag );
    if( ifail != OK ) {
        std::stringstream msg;
        msg << "The \"to\" class (" << cmd_to_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    ifail = POM_string_to_tag( cmd_from_uid.c_str(), &wrk_from_obj_tag );
    if( ifail != OK ) {
        std::stringstream msg;
        msg << "The \"from\" uid (" << cmd_from_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    ifail = POM_string_to_tag( cmd_to_uid.c_str(), &wrk_to_obj_tag );
    if( ifail != OK ) {
        std::stringstream msg;
        msg << "The \"to\" uid (" << cmd_to_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    if( op_fail != OK ) {
        return( op_fail );
    }

    /* Check uid's class against specified class */
    ifail = get_and_validate_class_tag( cmd_from_uid, cmd_from_class, wrk_from_class, &wrk_from_class_tag, &wrk_from_validated, "-from=" );

    if( ifail != OK ) 
    {
        return( ifail );
    }

    ifail = get_and_validate_class_tag( cmd_to_uid, cmd_to_class, wrk_to_class, &wrk_to_class_tag, &wrk_to_validated, "-to=");

    if( ifail != OK ) 
    {
        return( ifail );
    }

    /* get class CPID-s */
    wrk_from_cpid = get_cpid( wrk_from_class );
    wrk_to_cpid = get_cpid( wrk_to_class );

    if (wrk_from_cpid < 0 || wrk_to_cpid < 0) 
    {
        ifail = POM_invalid_string;

        if (wrk_from_cpid < 0)
        {
            std::stringstream msg;
            msg << "\nUnable to find class-ID (CPID) for class " << wrk_from_class;
            error_out(ERROR_line, ifail, msg.str());
        }

        if (wrk_to_cpid < 0)
        {
            std::stringstream msg;
            msg << "\nUnable to find class-ID (CPID) for class " << wrk_to_class;
            error_out(ERROR_line, ifail, msg.str());
        }
        return ( ifail );
    }

    /* finally, check references between from:uid & to:uid*/
    ifail = find_ref_count( wrk_from_cpid, cmd_from_uid, cmd_to_uid, wrk_to_cpid, &ref_count );

    if (ifail != OK)
    {
        return(ifail);
    }

    std::stringstream msg3;
    msg3 << "Found <" << ref_count << "> references between " << cmd_from_uid << " and " << cmd_to_uid;
    cons_out( msg3.str() );
    if (ref_count < 1) {
        std::stringstream msg;
        msg << "ZERO references found for this from-uid/to-uid pair; nothing to load or lock.";
        cons_out( msg.str() );
        return ( ifail );
    }
    logical doLoadToUid = TRUE;
    if( cmd_from_uid == cmd_to_uid ) {
        std::stringstream msg;
        msg << "from:uid same as to:uid; only 1 object to load/lock";
        cons_out( msg.str() );
        doLoadToUid = FALSE;
    }

    /* Check object(s) can be loaded, locked, unlocked, and unloaded */
    ifail = lockAndUnlock(wrk_from_obj_tag, wrk_from_class_tag, cmd_from_uid);
    if (ifail == OK && doLoadToUid == TRUE) {
        ifail = lockAndUnlock(wrk_to_obj_tag, wrk_to_class_tag, cmd_to_uid);
    }
    return( ifail );
}


/*
** Here we validate that the supplied class is the appropriate class.
** Query the database to get the object's true class and if it is different from 
** the specified class then use the object's class and get a new class tag.
*/
static int get_and_validate_class_tag( const std::string uid, const std::string target_class, std::string &found_class, tag_t* classTag, logical* db_validated, const char *option )
{
    *db_validated = false;

    int ifail = query_class_to_find_class_of_uid(uid, target_class, found_class );

    if (ifail != OK || found_class.empty())
    {
        ifail = query_class_to_find_class_of_uid(uid, "POM_object", found_class);
    }

    if (ifail != OK || found_class.empty()) 
    {
        std::stringstream msg;
        msg << "WARNING: Unable to find uid (" << uid << ") in specified class (" << target_class << "), wrong class -or- missing object?";
        cons_out( msg.str() );
        std::string ret = target_class;
        found_class = ret;
    }
    else 
    {
        if( !found_class.empty() && found_class.compare( target_class ) != 0 ) 
        {
            std::stringstream msg;
            msg << "WARNING: The specified class \"" << target_class << "\" ";

            if( option != NULL )
            {
                 msg << "(see the " << option << " option) " ;
            }

            msg << "does not match the object's class \"" << found_class << "\" - switching to object's class";
            cons_out( msg.str() );

            ifail = POM_class_id_of_class(found_class.c_str(), classTag );

            if( ifail != OK ) 
            {
                std::stringstream msg;
                msg << "The ";

                if( option != NULL )
                {
                    msg << "(" << option << ") ";
                }
                msg << "object's class (" << found_class << ") is invalid";
                error_out( ERROR_line, ifail, msg.str() );
            }
            else
            {
                *db_validated = true;
            }
        }
        else 
        {
            std::string ret = target_class;
            found_class = ret;
            *db_validated = true;
        }
    }
    return ifail;
}


static int lockAndUnlock(tag_t objTag, tag_t classTag, const std::string uid) {
    int ifail = OK;
    std::string lock_modes;
    if( !loadToNoLock(objTag, classTag )) {
        lock_modes = "<Unable to load object, see syslog for details>";
        return -1;
    }
    lock_modes = "no-lock";
    if (refreshToLock(objTag, POM_modify_lock, classTag )) {
        lock_modes = "locked";
    }
    else {
        std::stringstream msg;
        msg << "Object Lock failed!";
        cons_out( msg.str() );
        return -1;
    }
    std::stringstream msg4;
    msg4 << "Load results for uid=" << uid << " (tag=" << objTag << "): " << lock_modes;
    cons_out( msg4.str() );
    if (refreshToLock( objTag, POM_no_lock, classTag )) {
        std::stringstream msg;
        msg << "Unlock successful.";
        cons_out( msg.str() );
    }
    else {
        std::stringstream msg;
        msg << "Object Unlock failed!";
        cons_out( msg.str() );
    }
    if (unload( objTag )) {
        std::stringstream msg;
        msg << "Unload successful.";
        cons_out( msg.str() );
    }
    else {
        std::stringstream msg;
        msg << "Object Unload failed!";
        cons_out( msg.str() );
        return -1;
    }
    return ifail;
}


static int find_ref_op( int* found_count )
{
    int ifail = OK;
    /* Get class hierarchy */
    std::vector< hier_t > hier;

    getHierarchy( hier );

    /* Get system metadata of all typed and untyped references */
    std::vector< cls_t > meta;

    getRefMeta( meta, hier );

    if( args->debug_flag ) 
    {
        dumpHierarchy( hier ); 
        dumpRefMetadata( meta );
    }

    /* Loop through each class and each reference    */
    /* looking for a reference to the specified UID. */
    int ref_cnt = 0;
    int class_cnt = meta.size();
    int att_processed = 0;
    int flat_att_processed = 0;
    char *last_class = NULL;
    char *last_att   = NULL;
    int  lcl_ifail   = OK;

    if( !args->class_obj_flag )
    {
        /* Process all loaded attributes. */
        for( int i = 0; i < class_cnt && ref_cnt <= args->max_ref_cnt; i++)
        {
            last_class = meta[i].name;

            // Search normal class attributes and output the information.
            lcl_ifail = output_refs( &meta[i], &ref_cnt, &att_processed, &last_att );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }

            // Search flattened attributes and output the information.
            lcl_ifail = output_flattened_refs(  hier, meta, &meta[i], &ref_cnt, &flat_att_processed );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }
        }
    }
    else
    {
        /* The class_obj_flag indicates that we are looking for a reference in a specific object hierarchy. */
        int starting_cpid = get_cpid(args->class_obj_n);

        if (starting_cpid <= 0)
        {
            ifail = POM_invalid_string;
            std::stringstream msg;
            msg << "The class name on the -o option (" << args->class_obj_n << ") is invalid";
            ifail = error_out(ERROR_line, ifail, msg.str());
        }
        else
        { 
            /* process the specified class and its parent classes. */
            cls_t* class_p = NULL;

            /* Move up the hiearchy to the first class that has references */
            int cls_id = starting_cpid;
            hier_t* hptr = NULL;

            while (cls_id > 0)
            {
                hptr = &hier[cls_id];

                /* Does class have attributes with references?*/
                if (hptr->refs > 0 && hptr->cls_pos >= 0)
                {
                    class_p = &meta[hptr->cls_pos];
                    break;
                }
                cls_id = hptr->par_id;
            }

            /* Did we find a class that contains a reference attribute?*/
            if (class_p == NULL)
            {
                cons_out("\nNo classes were found that contain reference attributes.");
            };

            /* Query the reference attribute and then move up the hiearchy. */
            while (class_p != NULL)
            {
                last_class = class_p->name;

                // Search normal class attributes and output the information.
                lcl_ifail = output_refs(class_p, &ref_cnt, &att_processed, &last_att);

                if (ifail == OK && lcl_ifail != OK)
                {
                    ifail = lcl_ifail;
                }

                // Search flattened attributes and output the information.
                lcl_ifail = output_flattened_refs( hier, meta, class_p, &ref_cnt, &flat_att_processed );

                if (ifail == OK && lcl_ifail != OK)
                {
                    ifail = lcl_ifail;
                }

                //
                // Move to next class up the hierarchy.
                //
                cls_id = class_p->cls_id;
                class_p = NULL;
                hier_t* hptr = NULL;

                while (cls_id > 0)
                {
                    hptr = &hier[cls_id];
                    cls_id = hptr->par_id;
                    hptr = &hier[cls_id];

                    if (hptr->refs > 0 && hptr->cls_pos >= 0)
                    {
                        class_p = &meta[hptr->cls_pos];
                        break;
                    }
                }
            }
        }
    }

    if( last_class != NULL && last_att != NULL )
    {
        std::stringstream msg;
        msg << "\nLast attribute processed is " << last_class << ":" << last_att;
        cons_out( msg.str() );
    }

    *found_count = ref_cnt;
    std::stringstream msg;
    msg << "\nTotal system reference attributes         = " << args->att_cnt;
    msg << "\nNormal reference attributes processed     = " << att_processed;
    msg << "\nFlattened reference attributes processed  = " << flat_att_processed;
    msg << "\nTotal references found                    = " << ref_cnt;
    cons_out( msg.str() );

    return( ifail );
}


static int find_ext_ref_op()
{
    int ifail = OK;
    args->ext_ref_flag = TRUE;

    /* Get class hierarchy */
    std::vector< hier_t > hier;
    
    getHierarchy( hier );

    /* Get system metadata of all external references */
    std::vector< cls_t > meta;

    getExtRefMeta( meta, hier );

    if( args->debug_flag ) 
    {
        dumpHierarchy( hier ); 
        dumpRefMetadata( meta );
    }

    /* Loop through each class and each external reference  */
    /* looking for a reference to the specified UID.        */
    int ref_cnt = 0;
    int class_cnt = meta.size();
    int att_processed = 0;
    int flat_att_processed = 0;
    char *last_class = NULL;
    char *last_att   = NULL;
    int  lcl_ifail   = OK;

    if( !args->class_obj_flag )
    {
        /* Process all loaded attributes. */
        for( int i = 0; i < class_cnt && ref_cnt <= args->max_ref_cnt; i++)
        {
            last_class = meta[i].name;

            // Search normal class attributes and output the information.
            lcl_ifail = output_refs( &meta[i], &ref_cnt, &att_processed, &last_att );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }

            // Search flattened attributes and output the information.
            lcl_ifail = output_flattened_refs(  hier, meta, &meta[i], &ref_cnt, &flat_att_processed );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }
        }
    }
    else
    {
        /* process the specified class and its parent classes. */
        cls_t* class_p = NULL;

        for( int i = 0; i < class_cnt && ref_cnt <= args->max_ref_cnt; i++)
        {
            if( strcmp( meta[i].name, args->class_obj_n ) == 0 )
            {
                class_p = &meta[i];
                break;
            }
        }

        while( class_p != NULL )
        {
            last_class = class_p->name;

            // Search normal class attributes and output the information.
            lcl_ifail = output_refs( class_p, &ref_cnt, &att_processed, &last_att );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }

            // Search flattened attributes and output the information.
            lcl_ifail = output_flattened_refs(  hier, meta, class_p, &ref_cnt, &flat_att_processed );

            if( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }

            //
            // Move to next class up the hierarchy.
            //
            int cls_id   = class_p->cls_id;
            class_p      = NULL;
            hier_t* hptr = NULL;

            while( cls_id > 0 )
            {
                hptr   = &hier[cls_id];
                cls_id = hptr->par_id;
                hptr   = &hier[cls_id];

                if( hptr->refs > 0 && hptr->cls_pos >= 0 )
                {
                    class_p = &meta[hptr->cls_pos];
                    break;
                }
            }
        }
    }

    if( last_class != NULL && last_att != NULL )
    {
        std::stringstream msg;
        msg << "\nLast attribute processed is " << last_class << ":" << last_att;
        cons_out( msg.str() );
    }

    std::stringstream msg;
    msg << "\nTotal system reference attributes         = " << args->att_cnt;
    msg << "\nNormal reference attributes processed     = " << att_processed;
    msg << "\nFlattened references attributes processed = " << flat_att_processed;
    msg << "\nTotal references found                    = " << ref_cnt;
    cons_out( msg.str() );

    return( ifail );
}

static int find_class_op()
{
    cons_out( "" );
    int ifail = OK;
    std::vector< std::string >  *uid_vec_lcl = args->uid_vec;
    std::string class_name;
    
    if( args->class_flag )
    {
        class_name = args->class_n;
    }
   
    std::vector< std::string > unresolved_01;
    std::vector< std::string > unresolved_02;
    std::map< std::string, std::string > found_classes;
    std::vector< std::string > flattened_classes;

    if( args->uid_flag )
    {
        if( args->class_flag && args->class_n != NULL && strcmp( args->class_n, "POM_object" ) != 0 )
        {
            ifail = query_class_to_find_class_of_uid( (*uid_vec_lcl), class_name, found_classes );
        }

        unresolved_01.clear();
        ifail = find_unresolved( *uid_vec_lcl, found_classes, unresolved_01 );

        if( unresolved_01.size() > 0 )
        {
            ifail = query_class_to_find_class_of_uid( unresolved_01, "POM_object", found_classes );
        }

        unresolved_02.clear();
        ifail = find_unresolved( unresolved_01, found_classes, unresolved_02 );

        if( unresolved_02.size() > 0 )
        {
            ifail = get_flattened_class_names( flattened_classes );

            for( int j=0; j<flattened_classes.size(); j++ ) 
            {
                if( (j & 1) == 0 )
                {
                    ifail = query_class_to_find_class_of_uid( unresolved_02, flattened_classes[j].c_str(), found_classes );
                    unresolved_01.clear();
                    ifail = find_unresolved( unresolved_02, found_classes, unresolved_01 );

                    if( unresolved_01.size() == 0 )
                    {
                        break;
                    }
                }
                else
                {
                    ifail = query_class_to_find_class_of_uid( unresolved_01, flattened_classes[j].c_str(), found_classes );
                    unresolved_02.clear();
                    ifail = find_unresolved( unresolved_01, found_classes, unresolved_02 );

                    if( unresolved_02.size() == 0 )
                    {
                        break;
                    }
                }
            }
        }

        for( int i=0; i<uid_vec_lcl->size(); i++ )
        {
            std::string uid_class_name = "<not found>";

            std::map< std::string, std::string >::iterator it;
            it = found_classes.find( (*uid_vec_lcl)[i]);
            if( it != found_classes.end() )
            {
                uid_class_name = it->second;
            }

            std::stringstream msg;
            msg << "Class of uid (" <<  (*uid_vec_lcl)[i] << ") is " << uid_class_name;
            cons_out( msg.str() );
        }

    }
    else
    {
        cons_out( "The -uid=<uid> option is required with the -find_class operation" );
        ifail = FAIL; 
    }

    return( ifail );
}

static int find_stub_op( int* found_count )
{
    *found_count = -1;
    int ifail = OK;

    if ( !args->uid_flag )
    {
        cons_out( "The -uid=<uid> option is required with the -find_stub operation" );
        return FAIL;
    }

    std::vector<std::string>* uid_vec = args->uid_vec;
    output_stub_details( uid_vec, found_count );
    return ifail;
}

static int load_obj_op()
{
    int ifail = OK;
    std::vector< std::string >  *uid_vec_lcl = args->uid_vec;
    int uid_cnt = (*uid_vec_lcl).size();
    std::string class_name;
   
    std::vector< std::string > unresolved;
    std::map< std::string, std::string > found_classes;
    std::vector< std::string > flattened_classes;

    if (!args->class_flag && args->uid_flag)
    {
        /* Make the -c=class_name optional, if not specified search     */
        /* PPOM_OBJECT for the class name of the first UID in the list. */
        const std::string uid = (*uid_vec_lcl)[0];
        const std::string pom_object = "POM_object";

        if (!uid.empty())
        {
            ifail = query_class_to_find_class_of_uid(uid, pom_object, class_name);
        }

        if( class_name.empty() )
        {
            std::stringstream msg;
            msg << "Class name of uid (" << uid << ") could not be found, try specifing the -c=class_name option.";
            cons_out(msg.str());

            if (ifail == OK)
            {
                ifail = FAIL;
            }
        }
    }
    else
    {      
        /* Use the class name specified on the command line. */
        class_name = args->class_n;
    }

    if( args->uid_flag && ifail == OK )
    {
        logical attempt_load = true;
        tag_t   class_tag = NULL_TAG;

        ifail = query_class_to_find_class_of_uid( (*uid_vec_lcl), class_name, found_classes );
        unresolved.clear();
        ifail = find_unresolved( class_name, *uid_vec_lcl, found_classes, unresolved );

        tag_t *tag_list = (tag_t *)SM_alloc( sizeof( tag_t) * (uid_cnt + 1) );
        unsigned int prior;
        unsigned int post;

        for( int i=0; i<uid_cnt; i++ )
        {
            ifail = POM_string_to_tag( (*uid_vec_lcl)[i].c_str(), &(tag_list[i]) );
        }

        // Register the CPIDs of objects being loaded
        DMS_cache_cpids_of_class( uid_cnt, tag_list, class_name.c_str(), &prior, &post );

        if( post != (unsigned int)uid_cnt )
        {
            std::stringstream msg;
            msg << "Unable to cache (" << (uid_cnt - post) << ") CPIDs of objects associated with class " << class_name;
            cons_out( msg.str() );
            attempt_load = false;
        }

        if( args->force_flag )
        {
            ifail = POM_class_id_of_class( class_name.c_str(), &class_tag );
        }

        if( (attempt_load && unresolved.size() == 0) || args->force_flag )
        {
            for( int i = 0; i<uid_cnt; i++ )
            {
                std::string lock_modes; 

                if (!args->force_flag)
                {
                    ifail = POM_class_id_of_class(OM_ask_class_name(DDS_class_id_of_pid(DMS_get_cpid_from_cache(tag_list[i]))), &class_tag);
                }

                if( !loadToNoLock( tag_list[i], class_tag ) )
                {
                    lock_modes = "<Unable to load object, see syslog for details>";
                }
                else
                {
                    lock_modes = "no-lock";

                    if( refreshToLock( tag_list[i], POM_read_lock, class_tag ) )
                    {
                        lock_modes += "/R-lock";
                    }

                    if( refreshToLock( tag_list[i], POM_modify_lock, class_tag ) )
                    {
                        lock_modes += "/M-lock";
                    }

                    if( refreshToLock( tag_list[i], POM_delete_lock, class_tag ) )
                    {
                        lock_modes += "/D-lock";
                    }

                    if( unload( tag_list[i] ) )
                    {
                        lock_modes += "/unload";
                    }
                }

                std::stringstream msg;
                msg << "Load results for uid=" <<  (*uid_vec_lcl)[i] << " (tag=" << tag_list[i] << "): " << lock_modes;
                cons_out( msg.str() );
            }
        }
        else
        {
            // List contains uids that are not of the specified class.
            cons_out( "The following objects are not of the specified class, consider -find_class option to identify the correct class" );

            for( int i=0; i<unresolved.size(); i++ )
            {
                std::string uid_class_name = "<unknown>";

                std::map< std::string, std::string >::iterator it;
                it = found_classes.find( unresolved[i] );
                if( it != found_classes.end() )
                {
                    uid_class_name = it->second;
                }

                std::stringstream msg;
                msg << "Class of uid (" << unresolved[i] << ") is " << uid_class_name << " and is not " << class_name;
                cons_out( msg.str() );
            }
        }

    }
    else
    {
        if (!args->uid_flag)
        {
            cons_out("The -uid=<uid> option is required with the -load_obj operation");
        }
              
        ifail = FAIL; 
    }

    return( ifail );
}

static int add_ref_op()
{
    int op_fail = OK;
    int ifail = OK;
    std::string from_class;
    std::string from_attribute;
    std::string from_uid;
    tag_t       from_class_tag = NULL_TAG;
    tag_t       from_attr_tag = NULL_TAG;
    tag_t       from_tag = NULL_TAG;
    logical     from_m_locked = FALSE;
    int         from_desc = 0;
    int         from_type = 0;
    int         from_len  = 0;
    int         from_pos  = -1;

    std::string to_class;
    std::string to_uid;
    tag_t       to_class_tag = NULL_TAG;
    tag_t       to_tag = NULL_TAG;
    logical     to_r_locked = FALSE;
    logical     self_reference = FALSE;
    logical     work_done = FALSE;

    if( !args->from_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-add_ref option requires the \"-from=class:attribute:uid\" parameter " );
    }
    else
    {
        from_class = getSubParameter( args->from, 0 );

        if( from_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a class subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        from_attribute = getSubParameter( args->from, 1 );

        if( from_attribute.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires an attribute subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        from_uid = getSubParameter( args->from, 2);

        if( from_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a uid subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        const std::string from_pos_optional_val = getSubParameter( args->from, 3 );
        if ( ! from_pos_optional_val.empty() )
        {
            std::istringstream from_pos_stream(from_pos_optional_val);
            from_pos_stream >> from_pos;
        }
    }


    if( !args->to_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-add_ref option requires the \"-to=class:uid\" parameter " );
    }
    else
    {
        to_class = getSubParameter( args->to, 0 );

        if( to_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a class subparameter - E.g. \"-to=class:uid\"" );
        }

        to_uid = getSubParameter( args->to, 1);

        if( to_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a uid subparameter - E.g. \"-to=class:uid\"" );
        }
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    ifail = POM_class_id_of_class( from_class.c_str(), &from_class_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" class (" << from_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    else
    {
        // validate attribute here.
        ifail = POM_attr_id_of_attr( from_attribute.c_str(), from_class.c_str(), &from_attr_tag );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "The \"from\" class (" << from_class << ") does not contain an attribute named \"" << from_attribute << "\"";
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }
        else 
        {
            char** lcl_names = NULL;
            int*   lcl_types = NULL;
            int*   lcl_str_lengths = NULL;
            tag_t* lcl_ref_classes = NULL;
            int*   lcl_lengths = NULL;
            int*   lcl_descr = NULL;
            int*   lcl_failures = NULL;

            ifail = POM_describe_attrs( from_class_tag, 1, &from_attr_tag, &lcl_names, &lcl_types, &lcl_str_lengths, &lcl_ref_classes, &lcl_lengths, &lcl_descr, &lcl_failures );

            if( ifail != OK )
            {
                std::stringstream msg;
                msg << "Unable to describe attribute " << from_class << "." << from_attribute;
                op_fail = error_out( ERROR_line, ifail, msg.str() );
            }
            else
            {
                from_desc = *lcl_descr;
                from_type = *lcl_types;
                from_len  = *lcl_lengths;

                SM_free( (void *) lcl_names );
                SM_free( (void *) lcl_types );
                SM_free( (void *) lcl_str_lengths );
                SM_free( (void *) lcl_ref_classes );
                SM_free( (void *) lcl_lengths );
                SM_free( (void *) lcl_descr );
                SM_free( (void *) lcl_failures );
            }
        }
    }

    ifail = POM_class_id_of_class( to_class.c_str(), &to_class_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" class (" << to_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    ifail = POM_string_to_tag( from_uid.c_str(), &from_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" uid (" << from_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    ifail = POM_string_to_tag( to_uid.c_str(), &to_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" uid (" << to_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    if( from_type != POM_untyped_reference && from_type != POM_typed_reference )
    {
        std::stringstream msg;
        msg << "The \"from\" attribute (" << from_attribute << ") must be either a \"typed\" or \"untyped\" reference attribute";
        op_fail = error_out( ERROR_line, POM_attr_not_a_reference, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }


    // Load the "from" (referencing) object and modify lock it.
    if( loadToNoLock( from_tag, from_class_tag ) )
    {
        from_m_locked = refreshToLock( from_tag, POM_modify_lock, from_class_tag );
    }

    if( !from_m_locked )
    {
        std::stringstream msg;
        msg << "Unable to modify lock the referencing object (" << from_uid << ")";
        op_fail = error_out( ERROR_line, POM_failed_to_lock, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    // Load the "to" (referenced) object and read lock it,
    // provided that the "to" object is distinct from the "from"
    // object.
    //
    // Jumping through extra hoops here under the assumption that we
    // shouldn't attempt to load an object more than once, much less
    // lock the object more than once.
    self_reference = to_tag == from_tag && to_class_tag == from_class_tag;

    if ( !self_reference )
    {
        if( !loadToNoLock( to_tag, to_class_tag ) )
        {
            std::stringstream msg;
            msg << "Unable to load the referenced object (" << to_uid << ")";
            op_fail = error_out( ERROR_line, POM_inst_not_loaded, msg.str() );
        }
        else
        {
            to_r_locked = refreshToLock( to_tag, POM_read_lock, to_class_tag );
            if ( !to_r_locked )
            {
                std::stringstream msg;
                msg << "Unable to read lock the referenced object (" << to_uid << ")";
                op_fail = error_out( ERROR_line, POM_failed_to_lock, msg.str() );
            }
        }
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    // Sanity checks to ensure that the optional from_pos value matches
    // the attribute type (scalar, small array, large array, variable length).

    // Is a position given for a scalar attribute?
    if ( from_len == 1 && from_pos > -1 )
    {
        std::stringstream msg;
        msg << "Non-zero position (" << from_pos << ") given for scalar attribute " << from_attribute;
        op_fail = error_out( ERROR_line, POM_value_out_of_bounds, msg.str() );
    }
    // Is a position missing for small array or large array attribute?
    else if ( from_len > 1 && from_pos == -1 )
    {
        std::stringstream msg;
        msg << "Position missing for small array / large array attribute " << from_attribute;
        op_fail = error_out( ERROR_line, POM_value_out_of_bounds, msg.str() );
    }
    // Is the position in range for small array attribute?
    else if ( from_len > 1 && from_len < 7 && (from_pos < 0 || from_pos >= from_len) )
    {
        std::stringstream msg;
        msg << "Invalid position (" << from_pos << ") for small array attribute " << from_attribute;
        op_fail = error_out( ERROR_line, POM_value_out_of_bounds, msg.str() );
    }


    if( op_fail != OK )
    {
        return( op_fail );
    }

    op_fail = database_tx_check();

    if (op_fail != OK)
    {
        return(op_fail);
    }

    START_WORKING_TX( add_ref_tx, "add_ref_tx" );

    ERROR_PROTECT

    if( from_len == 1 )
    {
        POM_set_attr_tag( 1, &from_tag, from_attr_tag, to_tag );

        std::stringstream msg;
        msg << "Added one reference to " << to_uid;
        cons_out( msg.str() );
        work_done = TRUE;
    }
    else if( from_len > 1 && from_len < 7 )
    {
        // Process small arrays
        POM_set_attr_tags( 1, &from_tag, from_attr_tag, from_pos, 1, &to_tag );

        std::stringstream msg;
        msg << "There was 1 small-array reference to " << to_uid << " that was added";

        cons_out( msg.str() );
        work_done = TRUE;
    }
    else if( from_len >= 7 )
    {
        // Process large arrays
        POM_set_attr_tags( 1, &from_tag, from_attr_tag, from_pos, 1, &to_tag );

        std::stringstream msg;
        msg << "There was 1 long-array reference to " << to_uid << " that was added";
        cons_out( msg.str() );
        work_done = TRUE;
    }
    else if( from_len == -1 )
    {
        int vla_len = 0;

        // Process variable length arrays
        POM_length_of_attr ( from_tag, from_attr_tag, &vla_len);  

        if( vla_len >= 0 && from_pos >= vla_len )
        {
            std::stringstream msg;
            msg << "Invalid position (" << from_pos << ") exceeds length (" << vla_len << ") of variable length array attribute " << from_attribute;
            op_fail = error_out( ERROR_line, POM_value_out_of_bounds, msg.str() );
        }
        else if( vla_len >= 0 && from_pos >= 0 )
        {
            // Update exising reference.
            POM_set_attr_tags( 1, &from_tag, from_attr_tag, from_pos, 1, &to_tag );

            std::stringstream msg;
            msg << "There was 1 VLA reference to " << to_uid << " that was added";
            cons_out( msg.str() );
            work_done = TRUE;
        }
        else
        {
            // Add new reference to end of VLA.
            POM_append_attr_tags( 1, &from_tag, from_attr_tag, 1, &to_tag );

            std::stringstream msg;
            msg << "There was 1 VLA reference to " << to_uid << " that was added";
            cons_out( msg.str() );
            work_done = TRUE;
        }
    }
    else   
    {
        // Unknown array length
        std::stringstream msg;
        msg << "Unkown array-length/array-type assoociated with " << from_attribute << "";
        op_fail = error_out( ERROR_line, POM_internal_error, msg.str() );
    }


    // Clean up and get out

    if( op_fail == OK )
    {
        ifail = POM_save_instances( 1, &from_tag, false );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "Unable to save instance (" << from_uid << ")/(" << from_tag << ")";
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }
    }

    ERROR_RECOVER

    if( op_fail == OK )
    {
        op_fail = ERROR_ask_failure_code();
    }
    ERROR_END

    // Commit the work only if it was successful and the user said to commit on the command line.
    if ( ( op_fail == OK ) && args->commit_flag && work_done )
    {
        COMMIT_WORKING_TX( add_ref_tx, "add_ref_tx" );
    }
    else
    {
        if( op_fail == OK && args->commit_flag && !work_done )
        {
            std::stringstream msg;
            msg << "No reference was added, rolling back transaction.";
            cons_out( msg.str() );
        }
        else if( op_fail == OK && !args->commit_flag )
        {
            std::stringstream msg;
            msg << "-commit option NOT specified, rolling back transaction.";
            cons_out( msg.str() );

            op_fail = POM_invalid_value;
        }

        ROLLBACK_WORKING_TX( add_ref_tx, "add_ref_tx" );
    }

    // Release locks in the reverse order of that in which they were acquired.
    if (! self_reference)
    {
        if( to_tag != NULL_TAG )
        {
            if( to_r_locked )
            {
                refreshToLock( to_tag, POM_no_lock, to_class_tag );
            }
            unload( to_tag );
            to_tag = NULL_TAG;
        }
    }

    if( from_tag != NULL_TAG )
    {
        if( from_m_locked )
        {
            refreshToLock( from_tag, POM_no_lock, from_class_tag );
        }
        unload( from_tag );
        from_tag = NULL_TAG;
    }

    return( op_fail );
}

static int delete_obj_op()
{
    int      op_fail = OK;
    logical  work_done = FALSE;
    logical  objs_loaded = FALSE;

    // Check for license if doing -commit
    if( args->commit_flag )
    {
        op_fail = licenseFor( "-delete_obj" );

        if( op_fail != OK ) 
        {
            return( op_fail );
        }
    }

    op_fail = database_tx_check();

    if (op_fail != OK)
    {
        return(op_fail);
    }

    std::vector< std::string >  *uid_vec_lcl = args->uid_vec;
    int uid_cnt = (*uid_vec_lcl).size();
       
    if( args->uid_flag )
    {
        tag_t *tag_list = (tag_t *)SM_alloc( sizeof( tag_t) * (uid_cnt + 1) );

        // Convert the uids to tags.
        for( int i=0; ( ( i < uid_cnt ) && ( op_fail == POM_ok ) ); i++ )
        {
            op_fail = POM_string_to_tag( (*uid_vec_lcl)[i].c_str(), &(tag_list[i]) );
        }

        START_WORKING_TX( delete_obj_tx, "delete_obj_tx" );

        ERROR_PROTECT

        // If the class_flag was used to specify the class of all the objects, use it when loading the objects.
        // Otherwise, load the instances without the class specified.
        if ( args->class_flag )
        {
            const std::string class_name = args->class_n;
            tag_t class_tag = NULLTAG;
            op_fail = POM_class_id_of_class( class_name.c_str(), &class_tag );
            if ( op_fail != POM_ok )
            {
                const std::string msg( "Unable to obtain class id of instance." );
                cons_out( msg );
            }

            op_fail = POM_load_instances( uid_cnt, tag_list, class_tag, POM_delete_lock );
            if ( op_fail != POM_ok )
            {
                const std::string msg( "Unable to load an instance." );
                cons_out( msg );
            }

            if( op_fail == OK )
            {
                objs_loaded = TRUE;
            }
        }
        else
        {
            op_fail = POM_load_instances_any_class( uid_cnt, tag_list, POM_delete_lock );
            if ( op_fail != POM_ok )
            {
                const std::string msg( "Unable to load an instance." );
                cons_out( msg );
            }

            if( op_fail == OK )
            {
                objs_loaded = TRUE;
            }
        }



        // Delete the objects.
        if ( op_fail == POM_ok )
        {
            op_fail = POM_delete_instances( uid_cnt, tag_list );

            if( op_fail == POM_ok ) 
            {
                work_done = TRUE;
            }
            else
            {
                const std::string msg( "Unable to delete an instance." );
                cons_out( msg );
            }
        }

        ERROR_RECOVER

        if( op_fail == OK )
        {
            op_fail = ERROR_ask_failure_code();
        }
        ERROR_END

        // Commit the work only if it was successful and the user said to commit on the command line.
        if ( ( op_fail == OK ) && args->commit_flag && work_done )
        {
            COMMIT_WORKING_TX( delete_obj_tx, "delete_obj_tx" );
        }
        else
        {
            if( op_fail == OK && args->commit_flag && !work_done )
            {
                std::stringstream msg;
                msg << "No objects were deleted, rolling back transaction.";
                cons_out( msg.str() );
            }
            else if( op_fail == OK && !args->commit_flag )
            {
                std::stringstream msg;
                msg << "-commit option NOT specified, rolling back transaction.";
                cons_out( msg.str() );

                op_fail = POM_invalid_value;
            }
            else if( op_fail == POM_inst_referenced )
            {
                std::stringstream msg;
                msg << "At least one instance is referenced, rolling back transaction.";
                cons_out( msg.str() );
            }
            else 
            {
                std::stringstream msg;
                msg << "Error " << op_fail << " detected, rolling back transaction.";
                cons_out( msg.str() );
            }

            ROLLBACK_WORKING_TX( delete_obj_tx, "delete_obj_tx" );

            if( objs_loaded == TRUE )
            {
                int local_fail = POM_ok;
                local_fail = POM_load_instances( uid_cnt, tag_list, NULLTAG, POM_no_lock );

                if( local_fail != POM_ok )
                {
                    std::stringstream msg;
                    msg << "Unable to load instance \"" << tag_list[0] << "\" after rollback - ifail = " << local_fail << ".";
                    cons_out( msg.str() );
                }

                if( local_fail == POM_ok )
                {
                    local_fail = POM_unload_instances( uid_cnt, tag_list );

                    if( local_fail != POM_ok) 
                    {
                        std::stringstream msg;
                        msg << "Unable to unload instance \"" << tag_list[0] << "\" after rollback - ifail = " << local_fail << ".";
                        cons_out( msg.str() );
                    } 
                }

                if( op_fail == POM_ok )
                {
                   op_fail = local_fail;
                }
            }     
        }
    }
    else
    {
        cons_out( "The -uid=<uid> option is required with the -delete_obj operation" );
        
        op_fail = FAIL; 
    }

    return( op_fail );
}

static int remove_ref_op()
{
    int op_fail = OK;
    int ifail = OK;
    std::string from_class;
    std::string from_attribute;
    std::string from_uid;
    tag_t       from_class_tag = NULL_TAG;
    tag_t       from_attr_tag = NULL_TAG;
    tag_t       from_tag = NULL_TAG;
    logical     from_m_locked = FALSE;
    int         from_desc = 0;
    int         from_type = 0;
    int         from_len  = 0;
    int         from_pos  = -1;
    logical     work_done = FALSE;

    std::string to_class;
    std::string to_uid;
    tag_t       to_class_tag = NULL_TAG;
    tag_t       to_tag = NULL_TAG;

    // Check for license if doing commit.
    if( args->commit_flag )
    {
        op_fail = licenseFor( "-remove_ref" );

        if( op_fail != OK ) 
        {
            return( op_fail );
        }
    }

    // Parse the "from" arguments
    if( !args->from_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-remove_ref option requires the \"-from=class:attribute:uid[:pos]\" parameter " );
    }
    else
    {
        from_class = getSubParameter( args->from, 0 );

        if( from_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a class subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        from_attribute = getSubParameter( args->from, 1 );

        if( from_attribute.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires an attribute subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        from_uid = getSubParameter( args->from, 2);

        if( from_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a uid subparameter - E.g. \"-from=class:attribute:uid[:pos]\"" );
        }

        const std::string from_pos_optional_val = getSubParameter( args->from, 3 );
        if ( ! from_pos_optional_val.empty() )
        {
            std::istringstream from_pos_stream(from_pos_optional_val);
            from_pos_stream >> from_pos;
        }
    }

    // Parse the "to" arguments
    if( !args->to_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-remove_ref option requires the \"-to=class:uid\" parameter " );
    }
    else
    {
        to_class = getSubParameter( args->to, 0 );

        if( to_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a class subparameter - E.g. \"-to=class:uid\"" );
        }

        to_uid = getSubParameter( args->to, 1);

        if( to_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a uid subparameter - E.g. \"-to=class:uid\"" );
        }
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    // Check the "from" class.
    ifail = POM_class_id_of_class( from_class.c_str(), &from_class_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" class (" << from_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    else
    {
        // validate attribute here.
        ifail = POM_attr_id_of_attr( from_attribute.c_str(), from_class.c_str(), &from_attr_tag );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "The \"from\" class (" << from_class << ") does not contain an attribute named \"" << from_attribute << "\"";
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }
        else 
        {
            char** lcl_names = NULL;
            int*   lcl_types = NULL;
            int*   lcl_str_lengths = NULL;
            tag_t* lcl_ref_classes = NULL;
            int*   lcl_lengths = NULL;
            int*   lcl_descr = NULL;
            int*   lcl_failures = NULL;

            ifail = POM_describe_attrs( from_class_tag, 1, &from_attr_tag, &lcl_names, &lcl_types, &lcl_str_lengths, &lcl_ref_classes, &lcl_lengths, &lcl_descr, &lcl_failures );

            if( ifail != OK )
            {
                std::stringstream msg;
                msg << "Unable to describe attribute " << from_class << "." << from_attribute;
                op_fail = error_out( ERROR_line, ifail, msg.str() );
            }
            else
            {
                from_desc = *lcl_descr;
                from_type = *lcl_types;
                from_len  = *lcl_lengths;

                SM_free( (void *) lcl_names );
                SM_free( (void *) lcl_types );
                SM_free( (void *) lcl_str_lengths );
                SM_free( (void *) lcl_ref_classes );
                SM_free( (void *) lcl_lengths );
                SM_free( (void *) lcl_descr );
                SM_free( (void *) lcl_failures );
            }
        }
    }

    // Check the "to" class. 
    ifail = POM_class_id_of_class( to_class.c_str(), &to_class_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" class (" << to_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    else
    {
        OM_class_t class_id = POM_class_tag_to_om_id( 1, to_class_tag );

        //  if (OM_has_class_property( class_id, OM_class_prop_is_revisable ))  // Used by prior releases.
        if (OM_has_class_property( class_id, OM_class_prop_is_versionable ))
        {
            std::stringstream msg;
            msg << "The \"to\" class (" << to_class << ") is a versionable class - removing references to versionable objects is not supported ";
            op_fail = error_out( ERROR_line, POM_op_not_supported, msg.str() );
        }
    }

    // Check the "from" object
    ifail = POM_string_to_tag( from_uid.c_str(), &from_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" uid (" << from_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    // Check the "to" object
    ifail = POM_string_to_tag( to_uid.c_str(), &to_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" uid (" << to_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    if( from_type != POM_untyped_reference && from_type != POM_typed_reference )
    {
        std::stringstream msg;
        msg << "The \"from\" attribute (" << from_attribute << ") must be either a \"typed\" or \"untyped\" reference attribute";
        op_fail = error_out( ERROR_line, POM_attr_not_a_reference, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    if( from_len != 1 )
    {
        if( (from_pos >= 0 && args->all_flag) || (from_pos < 0 && !args->all_flag) )
        {
            std::stringstream msg;
            msg << "Either :pos or -all MUST be specified, but NOT both)";
            op_fail = error_out( ERROR_line, POM_invalid_value, msg.str());
        }
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    // Load the "from" (referencing) object and modify lock it.
    if( loadToNoLock( from_tag, from_class_tag ) )
    {
        if( refreshToLock( from_tag, POM_modify_lock, from_class_tag ) ) 
        {
            from_m_locked = TRUE;
        }
    }

    if( !from_m_locked )
    {
        std::stringstream msg;
        msg << "Unable to modify lock the referencing object (" << from_uid << ")";
        op_fail = error_out( ERROR_line, POM_failed_to_lock, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    op_fail = database_tx_check();

    if (op_fail != OK)
    {
        return(op_fail);
    }

    START_WORKING_TX( remove_ref_tx, "remove_ref_tx" );

    ERROR_PROTECT

    if( from_len == 1 )
    {

        tag_t refed_tag = null_tag;
        logical  is_null = FALSE;
        logical  is_empty = FALSE;
        POM_ask_attr_tag( from_tag, from_attr_tag, &refed_tag, &is_null, &is_empty );

        // Ensure we are removing the correct reference
        if( to_tag != refed_tag )
        {
            char *refed_uid = NULL;
            POM_tag_to_uid( refed_tag, &refed_uid );

            std::stringstream msg;
            msg << "The \"to\" UID (" << to_uid << ") does NOT match the referenced UID (" << refed_uid << ")";
            op_fail = error_out( ERROR_line, POM_invalid_value, msg.str() );

            SM_free( (void *) refed_uid );
        }
        else
        {
            if( (from_desc & POM_null_is_valid) == POM_null_is_valid && !args->null_ref_flag )
            {
                POM_set_attr_null( 1, &from_tag, from_attr_tag );
            }
            else
            {
                POM_set_attr_tag( 1, &from_tag, from_attr_tag, null_tag );
            }

            work_done = TRUE;
            std::stringstream msg;
            msg << "There was one reference to " << to_uid << " that was removed";
            cons_out( msg.str() );
        }
    }
    else if( from_len > 1 && from_len < 7 )
    {
        // Process small arrays
        int removal_cnt = 0;
        tag_t   nothing = null_tag;
        tag_t   *ret_tags = NULL;
        logical *is_null = NULL;
        logical *is_empty = NULL;

        POM_ask_attr_tags  ( from_tag, from_attr_tag, 0, from_len, &ret_tags, &is_null, &is_empty);

        for( int i=(from_len-1); i>=0; i-- )
        {
            if( i == from_pos || args->all_flag )
            {
                if( !is_null[i] && !is_empty[i] && ret_tags[i] == to_tag )
                {
                    if( (from_desc & POM_null_is_valid) == POM_null_is_valid )
                    {
                        POM_set_attr_nulls( 1, &from_tag, from_attr_tag, i, 1 );
                    }
                    else
                    {
                        POM_set_attr_tags( 1, &from_tag, from_attr_tag, i, 1, &nothing );
                    }
                    removal_cnt++;
                    work_done = TRUE;
                }
            }
        }

        SM_free( (void *) ret_tags );
        SM_free( (void *) is_null  );
        SM_free( (void *) is_empty );

        std::stringstream msg;
        if( removal_cnt != 1 )
        {
            msg << "There were " << removal_cnt << " small-array refernces to " << to_uid << " that were removed";
        }
        else
        {
            msg << "There was " << removal_cnt << " small-array refernce to " << to_uid << " that was removed";
        }

        cons_out( msg.str() );
    }
    else if( from_len >= 7 )
    {
        // Process large arrays
        int removal_cnt = 0;
        tag_t   nothing = null_tag;
        tag_t   *ret_tags = NULL;
        logical *is_null = NULL;
        logical *is_empty = NULL;

        POM_ask_attr_tags  ( from_tag, from_attr_tag, 0, from_len, &ret_tags, &is_null, &is_empty);

        for( int i=(from_len-1); i>=0; i-- )
        {
            if( i == from_pos || args->all_flag )
            {
                if( !is_null[i] && !is_empty[i] && ret_tags[i] == to_tag )
                {
                    if( (from_desc & POM_null_is_valid) == POM_null_is_valid )
                    {
                        POM_set_attr_nulls( 1, &from_tag, from_attr_tag, i, 1 );
                    }
                    else
                    {
                        POM_set_attr_tags( 1, &from_tag, from_attr_tag, i, 1, &nothing );
                    }
                    removal_cnt++;
                    work_done = TRUE;
                }
            }
        }

        SM_free( (void *) ret_tags );
        SM_free( (void *) is_null  );
        SM_free( (void *) is_empty );

        std::stringstream msg;
        if( removal_cnt != 1 )
        {
            msg << "There were " << removal_cnt << " long-array refernces to " << to_uid << " that were removed";
        }
        else
        {
            msg << "There was " << removal_cnt << " long-array refernce to " << to_uid << " that was removed";
        }
        cons_out( msg.str() );
    }
    else if( from_len == -1 )
    {
        int removal_cnt = 0;
        int vla_len = 0;
        tag_t   *ret_tags = NULL;
        logical *is_null = NULL;
        logical *is_empty = NULL;

        // Process variable length arrays
        POM_length_of_attr ( from_tag, from_attr_tag, &vla_len);  

        if( vla_len > 0 )
        {
            POM_ask_attr_tags  ( from_tag, from_attr_tag, 0, vla_len, &ret_tags, &is_null, &is_empty);

            for( int i=(vla_len-1); i>=0; i-- )
            {
                if( i == from_pos || args->all_flag )
                {
                    if( !is_empty[i] && ret_tags[i] == to_tag )
                    {
                        POM_remove_from_attr( 1, &from_tag, from_attr_tag, i, 1 );
                        removal_cnt++;
                        work_done = TRUE;
                    }
                }
            }

            SM_free( (void *) ret_tags );
            SM_free( (void *) is_null  );
            SM_free( (void *) is_empty );

            std::stringstream msg;
            if( removal_cnt != 1 )
            {
                msg << "There were " << removal_cnt << " VLA refernces to " << to_uid << " that were removed";
            }
            else
            {
                msg << "There was " << removal_cnt << " VLA refernce to " << to_uid << " that was removed";
            }
            cons_out( msg.str() );
        }
        else
        {
            std::stringstream msg;
            msg << "There are no VLA entries to check - 0 references to " << to_uid << " were removed";
            cons_out( msg.str() );
        }
    }
    else   
    {
        // Unknown array length
        std::stringstream msg;
        msg << "Unkown array-length/array-type assoociated with " << from_attribute << "";
        op_fail = error_out( ERROR_line, POM_internal_error, msg.str() );
    }


    // Clean up and get out
    if( op_fail == OK )
    {
        ifail = POM_save_instances( 1, &from_tag, false );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "Unable to save instance (" << from_uid << ")/(" << from_tag << ")";
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }
    }

    ERROR_RECOVER

    if( op_fail == OK )
    {
        op_fail = ERROR_ask_failure_code();
    }
    ERROR_END

    // Commit the work only if it was successful and the user said to commit on the command line.
    if ( ( op_fail == OK ) && args->commit_flag && work_done )
    {
        COMMIT_WORKING_TX( remove_ref_tx, "remove_ref_tx" );
    }
    else
    {
        if( op_fail == OK && args->commit_flag && !work_done )
        {
            std::stringstream msg;
            msg << "No references were removed, rolling back transaction.";
            cons_out( msg.str() );
        }
        else if( op_fail == OK && !args->commit_flag )
        {
            std::stringstream msg;
            msg << "-commit option NOT specified, rolling back transaction.";
            cons_out( msg.str() );

            op_fail = POM_invalid_value;
        }

        ROLLBACK_WORKING_TX( remove_ref_tx, "remove_ref_tx" );
    }

    if( from_tag != NULL_TAG )
    {
        if( from_m_locked )
        {
            refreshToLock( from_tag, POM_no_lock, from_class_tag );
        }
        unload( from_tag );
        from_tag = NULL_TAG;
    }

    return( op_fail );
}

static int validate_bp_op( Op op )
{
    return( validate_or_correct_bp( op ) );
}

static int correct_bp_op( Op op )
{
    return( validate_or_correct_bp( op ) );
}

static int validate_or_correct_bp( Op op )
{
    int op_fail = OK;
    int ifail = OK;

    std::string cmd_from_uid;
    std::string cmd_from_class;
    std::string wrk_from_class;
    tag_t       wrk_from_class_tag = NULL_TAG;
    tag_t       wrk_from_obj_tag = NULL_TAG;
    int         wrk_from_cpid = -1;
    logical     wrk_from_validated = false;
       
    logical     from_loaded = FALSE;
    logical     from_m_locked = FALSE;
   
    const std::string obj_class_to_search;
    int ref_count = 0;

    std::string cmd_to_uid;
    std::string cmd_to_class;
    std::string wrk_to_class;
    tag_t       wrk_to_class_tag = NULL_TAG;
    tag_t       wrk_to_obj_tag = NULL_TAG;
    int         wrk_to_cpid = -1;
    logical     wrk_to_validated = false;

    /* 
    ** Parse the input parameters to ensure that required input parameters are available. 
    */

    if( !args->from_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-validate_bp option requires the \"-from=class:uid\" parameter " );
    }
    else
    {
        cmd_from_class = getSubParameter( args->from, 0 );

        if( cmd_from_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a class subparameter - E.g. \"-from=class:uid\"" );
        }

        cmd_from_uid = getSubParameter( args->from, 1);

        if( cmd_from_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a uid subparameter - E.g. \"-from=class:uid\"" );
        }
    }


    if( !args->to_flag )
    {
        op_fail = error_out( ERROR_line, POM_invalid_value, "-validate_bp option requires the \"-to=class:uid\" parameter " );
    }
    else
    {
        cmd_to_class = getSubParameter( args->to, 0 );

        if( cmd_to_class.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a class subparameter - E.g. \"-to=class:uid\"" );
        }

        cmd_to_uid = getSubParameter( args->to, 1);

        if( cmd_to_uid.empty() )
        {
            op_fail = error_out( ERROR_line, POM_invalid_value, "The \"-to=...\" option requires a uid subparameter - E.g. \"-to=class:uid\"" );
        }
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }

    /*
    ** Here we validate that the specified classes are in fact actual classes.
    */
    ifail = POM_class_id_of_class( cmd_from_class.c_str(), &wrk_from_class_tag );

    if ( ifail != OK )
    {
        // Check if a CPID was specified for the class name. 
        std::string l_class_name = get_class_name_from_cpid( cmd_from_class );

        if ( !l_class_name.empty( ) )
        {
            ifail = POM_class_id_of_class( l_class_name.c_str( ), &wrk_from_class_tag );

            if ( ifail == OK )
            {
                cmd_from_class = l_class_name;
            }
        }
    }

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" class (" << cmd_from_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str( ) );
    }

    ifail = POM_class_id_of_class( cmd_to_class.c_str(), &wrk_to_class_tag );

    if ( ifail != OK )
    {
        // Check if a CPID was specified for the class name. 
        std::string l_class_name = get_class_name_from_cpid( cmd_to_class );

        if ( !l_class_name.empty( ) )
        {
            ifail = POM_class_id_of_class( l_class_name.c_str( ), &wrk_to_class_tag );

            if ( ifail == OK )
            {
                cmd_to_class = l_class_name;
            }
        }
    }

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" class (" << cmd_to_class << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str( ) );
    }

    ifail = POM_string_to_tag( cmd_from_uid.c_str(), &wrk_from_obj_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"from\" uid (" << cmd_from_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    ifail = POM_string_to_tag( cmd_to_uid.c_str(), &wrk_to_obj_tag );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "The \"to\" uid (" << cmd_to_uid << ") is invalid";
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }

    if( op_fail != OK )
    {
        return( op_fail );
    }
   
    /*
    ** Here we validate that the supplied class is the appropriate class.
    ** Query the database to get the object's true class and if it is different from 
    ** the specified class then use the object's class and get a new class tag.
    */
    ifail = get_and_validate_class_tag( cmd_from_uid, cmd_from_class, wrk_from_class, &wrk_from_class_tag, &wrk_from_validated, "-from=" );

    if( ifail != OK ) 
    {
        return( ifail );
    }

    ifail = get_and_validate_class_tag( cmd_to_uid, cmd_to_class, wrk_to_class, &wrk_to_class_tag, &wrk_to_validated, "-to=" );

    if( ifail != OK ) 
    {
        return( ifail );
    }

    /*
    ** Load the "from" (referencing) object and lock it with a modify-lock.
    */
    if( loadToNoLock( wrk_from_obj_tag, wrk_from_class_tag ) )
    {
        from_loaded = TRUE;

        if( refreshToLock( wrk_from_obj_tag, POM_modify_lock, wrk_from_class_tag ) ) 
        {
            from_m_locked = TRUE;
        }
    }

    if( op == validate_bp )
    {
        if( !from_loaded )
        {
            std::stringstream msg;
            msg << "\nNOTE: Unable to load the referencing object (" << cmd_from_uid << ") - Unable to validate the specified class is correct";
            cons_out( msg.str() );
        }

        if( !from_m_locked )
        {
            std::stringstream msg;
            msg << "\nNOTE: Unable to modify-lock the referencing object (" << cmd_from_uid << ") - Unable to prevent lock-count from changing (check syslog for details)";
            cons_out( msg.str() );
        }
    }
    else if ( op == correct_bp && !args->deleted_flag && !from_m_locked )
    {
            std::stringstream msg;
            msg << "\nERROR: Unable to modify-lock the referencing object (" << cmd_from_uid << ") - required in order to count references and update backpointers";
            op_fail = error_out( ERROR_line, POM_failed_to_lock, msg.str() );
    }

    if( op_fail != OK )
    {
        if( from_m_locked )
        {
            refreshToLock( wrk_from_obj_tag, POM_no_lock, wrk_from_class_tag );
        }
        return( op_fail );
    }

    /*
    ** Report references from the object and references from backpointers, 
    ** compare them and report status and recommendations
    */

    if( wrk_from_cpid < 1 )
    {
        wrk_from_cpid = get_cpid( wrk_from_class );
    }

    if( wrk_to_cpid < 1 )
    {
        wrk_to_cpid = get_cpid( wrk_to_class );
    }

    if (wrk_from_cpid < 0 || wrk_to_cpid < 0)
    {
        int ifail = POM_invalid_string;

        if (wrk_from_cpid < 0)
        {
            std::stringstream msg;
            msg << "\nUnable to find class-ID (CPID) for class " << wrk_from_class;
            op_fail = error_out(ERROR_line, ifail, msg.str());
        }

        if (wrk_to_cpid < 0)
        {
            std::stringstream msg;
            msg << "\nUnable to find class-ID (CPID) for class " << wrk_to_class;
            op_fail = error_out(ERROR_line, ifail, msg.str());
        }
        return (op_fail);
    }

    ifail = find_ref_count( wrk_from_cpid, cmd_from_uid, cmd_to_uid, wrk_to_cpid, &ref_count );

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "\nUnable to successfully count references from " << cmd_from_uid << " to " << cmd_to_uid;
        op_fail = error_out( ERROR_line, ifail, msg.str() );
    }
    else
    {
        cons_out("");
        report_object_refs( cmd_from_uid, wrk_from_cpid, wrk_from_class, cmd_to_uid, wrk_to_cpid, wrk_to_class, ref_count, cmd_from_class, cmd_to_class );
    }

    
    // if there is an actual object then error if the user asserted the object has been deleted.
    if( args->deleted_flag && ref_count > 0 )
    {
        std::stringstream msg;
        msg << "\n-deleted option can't be specified when the object contains " << ref_count << " references";
        op_fail = error_out( ERROR_line, POM_invalid_token, msg.str() );
    }

    std::vector< bp_t > bptrs;

    if (op_fail == OK && op == correct_bp)
    {
        op_fail = database_tx_check();
    }

    if( op_fail == OK && op == correct_bp )
    {
        logical work_done = FALSE;

        START_WORKING_TX( correct_bp_tx, "correct_bp_tx" );

        ERROR_PROTECT

        /* 
        ** CORRECT POM_BACKPOINTERS
        ** Validate the backpointers against the object before and after making the
        ** change to ensure that things are done appropriately.
        */
        ifail = get_backpointers( cmd_from_uid, cmd_to_uid, bptrs );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "\nUnable to access backpointers for from_uid = " << cmd_from_uid << " and to_uid = " << cmd_to_uid;
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }

        if (op_fail == OK)
        {
            cons_out("");
            cons_out("Backpointers PRIOR TO correction operation");
            report_backpointers(bptrs);
        }

        if (op_fail == OK && args->deleted_flag)
        {
            /* When working with a deleted object make sure that the classes exactly match. */
            /* When the classes match we know the previous code tried to load the objects   */
            /* and also attempt to count the references starting with the correct class.    */
            int fail_count = 0;

            for (int x = 0; x < bptrs.size(); x++)
            {
                if (!wrk_from_validated && bptrs[x].from_class != wrk_from_cpid)
                {
                    ifail = POM_invalid_value;

                    if (fail_count == 0)
                    {
                        std::stringstream msg;
                        msg << "\nWhen working with a deleted object the -from= class & -to= class must exactly match\nthe POM_BACKPOINTER.from_class & POM_backpointer.to_class";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }

                    std::stringstream msg;
                    msg << " -from= class = " << wrk_from_class << "(" << wrk_from_cpid << ")    POM_BACKPOINTER.from_class = " << bptrs[x].from_class_name << "(" << bptrs[x].from_class << ")";
                    cons_out(msg.str());

                    fail_count++;
                }

                if (!wrk_to_validated && bptrs[x].to_class != wrk_to_cpid)
                {
                    ifail = POM_invalid_value;

                    if (fail_count == 0)
                    {
                        std::stringstream msg;
                        msg << "\nWhen working with a deleted object the -from= class & -to= class must exactly match\nthe POM_BACKPOINTER.from_class & POM_backpointer.to_class";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }

                    std::stringstream msg;
                    msg << " -to= class = " << wrk_to_class << "(" << wrk_to_cpid << ")    POM_BACKPOINTER.to_class = " << bptrs[x].to_class_name << "(" << bptrs[x].to_class << ")";
                    cons_out(msg.str());

                    fail_count++;
                }
            }
        }

        if( op_fail == OK )
        {
            if( !compare_object_to_bp( cmd_from_uid, wrk_from_class, cmd_to_uid, wrk_to_class, ref_count,  bptrs ) )
            {

                ifail = adjust_backpointers( cmd_from_uid, wrk_from_cpid, cmd_to_uid, wrk_to_cpid, ref_count );

                if( ifail != OK )
                {
                    std::stringstream msg;
                    msg << "Unable to adjust POM_BACKPOINTER for from_uid = " << cmd_from_uid << " and to_uid = " << cmd_to_uid;
                    op_fail = error_out( ERROR_line, POM_internal_error, msg.str() );
                }

                if( op_fail == OK )
                {
                    bptrs.clear();
                    ifail = get_backpointers( cmd_from_uid, cmd_to_uid, bptrs );

                    if( ifail != OK )
                    {
                        std::stringstream msg;
                        msg << "Unable to access backpointers for from_uid = " << cmd_from_uid << " and to_uid = " << cmd_to_uid;
                        op_fail = error_out( ERROR_line, ifail, msg.str() );
                    }
                    else
                    {
                        cons_out("");
                        cons_out("Backpointers AFTER correction operation");
                        report_backpointers( bptrs );
                        work_done = TRUE;
                    }
                }
            }
        }

        ERROR_RECOVER

        if( op_fail == OK )
        {
            op_fail = ERROR_ask_failure_code();
        }
        ERROR_END

        // Commit the work only if it was successful and the user said to commit on the command line.
        if ( ( op_fail == OK ) && args->commit_flag && work_done )
        {
            COMMIT_WORKING_TX( correct_bp_tx, "correct_bp_tx" );
        }
        else
        {
            if( op_fail == OK && args->commit_flag && !work_done )
            {
                std::stringstream msg;
                msg << "\nNo backpointer adjustments were made, rolling back transaction.";
                cons_out( msg.str() );
            }
            else if( op_fail == OK && !args->commit_flag )
            {
                std::stringstream msg;
                msg << "\n-commit option NOT specified, rolling back transaction.";
                cons_out( msg.str() );

                op_fail = POM_invalid_value;
            }

            ROLLBACK_WORKING_TX( correct_bp_tx, "correct_bp_tx" );
        }
    }
   
    if( op_fail == OK && op == validate_bp )
    {
        /* 
        ** VALIDATE POM_BACKPOINTERS
        ** Validate the backpointers against the object.
        */
        ifail = get_backpointers( cmd_from_uid, cmd_to_uid, bptrs );

        if( ifail != OK )
        {
            std::stringstream msg;
            msg << "\nUnable to access backpointers for from_uid = " << cmd_from_uid << " and to_uid = " << cmd_to_uid;
            op_fail = error_out( ERROR_line, ifail, msg.str() );
        }
        else
        {
            report_backpointers( bptrs );
        }
    }

    if( op_fail == OK )
    {
        logical valid_backpointers = TRUE;
        int bp_count = bptrs.size();
        int bp_refs  = 0;

        cons_out( "" );

        if( bp_count > 1 )
        {
             std::stringstream msg;
             msg << "\nSTATUS: The number of POM_BACKPOINTER records (" << bp_count << ") is greater than 1";
             cons_out( msg.str() );
             valid_backpointers = FALSE;
        }

        for( int i=0; i<bp_count; i++ )
        {
            bp_t *bp = &(bptrs[i]);
            bp_refs  += bp->bp_count;
        }

        if( bp_refs != ref_count )
        {
             std::stringstream msg;
             msg << "\nSTATUS: The number of object references (" << ref_count << ") does not match the number of backpointer references (" << bp_refs << ")";
             cons_out( msg.str() );
             valid_backpointers = FALSE;
        }

        for( int i=0; i<bp_count; i++ )
        {
            bp_t *bp = &(bptrs[i]);

            if( wrk_from_class.compare( bp->from_class_name ) != 0 )
            {
                 std::stringstream msg;
                 msg << "\nSTATUS: The referencing object class (" << wrk_from_class << ") does not match the back-pointer from class (" << bp->from_class_name << ")";
                 cons_out( msg.str() );
                 valid_backpointers = FALSE;
                 break;
            }
        }

        for( int i=0; i<bp_count; i++ )
        {
            bp_t *bp = &(bptrs[i]);

            if( wrk_to_class.compare( bp->to_class_name ) != 0 )
            {
                 std::stringstream msg;
                 msg << "\nSTATUS: The referenced object class (" << wrk_to_class << ") does not match the back-pointer to class (" << bp->to_class_name << ")";
                 cons_out( msg.str() );
                 valid_backpointers = FALSE;
                 break;
            }
        }

        if( valid_backpointers && ( from_loaded || (!from_loaded && args->deleted_flag) ) )
        {
            std::stringstream msg;
            msg << "\nSTATUS: Object references match backpointer content - everything VALIDATED";
            cons_out( msg.str() );

            msg.str("");
            msg << "ACTION: none";
            cons_out( msg.str() );
        }
        else 
        {
            int action_point = 1;
            std::stringstream msg;

            if( !from_loaded && !args->deleted_flag )
            {
                msg.str("");
                msg << "\nACTION: Take one of the following corrective actions";
                cons_out( msg.str() );
                msg.str("");
                msg << "    " << action_point << "  Identify exact class of referencing object so that it can be loaded and references counted";
                cons_out( msg.str() );
                msg.str("");
                msg << "       " << args->root_exe << " -find_class -u=<> -p=<> -g=<> -c=" << wrk_from_class << "  -uid=" << cmd_from_uid;
                cons_out( msg.str() );
                action_point++;

                msg.str("");
                msg << "    " << action_point << "  Assert object has been deleted and re-validate";
                cons_out( msg.str() );
                msg.str("");
                msg << "       " << args->root_exe << " -validate_bp -u=<> -p=<> -g=<> -from=" << wrk_from_class << ":" << cmd_from_uid << " -to=" << wrk_to_class << ":" << cmd_to_uid << " -deleted";
                cons_out( msg.str() );
                action_point++;
            }
            else if( !from_loaded && args->deleted_flag )
            {
                msg.str("");
                msg << "\nACTION: Take the following corrective action to remove backpointer content";
                cons_out( msg.str() );
                msg.str("");
                msg << "        " << args->root_exe << " -correct_bp -u=<> -p=<> -g=<> -from=" << wrk_from_class << ":" << cmd_from_uid << " -to=" << wrk_to_class << ":" << cmd_to_uid << " -deleted";
                cons_out( msg.str() );
                action_point++;
            }
            else 
            {
                msg.str("");
                msg << "\nACTION: Take the following corrective action to correct the backpointer content";
                cons_out( msg.str() );
                msg.str("");
                msg << "        " << args->root_exe << " -correct_bp -u=<> -p=<> -g=<> -from=" << wrk_from_class << ":" << cmd_from_uid << " -to=" << wrk_to_class << ":" << cmd_to_uid;
                cons_out( msg.str() );
            }
        }
    }

    /*
    ** Clean up and get out. 
    */
    if( wrk_from_obj_tag != NULL_TAG )
    {
        if( from_m_locked )
        {
            refreshToLock( wrk_from_obj_tag, POM_no_lock, wrk_from_class_tag );
        }
    }

    if( from_loaded )
    {
        unload( wrk_from_obj_tag );
        wrk_from_obj_tag = NULL_TAG;
    }

    return( op_fail );
}

#define WHERE_REF_COL_CNT 6

static int where_ref_op()
{
    int op_fail = OK;

    if( args->where_ref_sub != 1 && args->where_ref_sub != 2 )
    {
        ERROR_raise(ERROR_line, POM_internal_error, "Invalid internal where-ref option = %d.", args->where_ref_sub);
    }

    if (args->where_ref_sub == 2 && !DDS_DB_object_exists("all_backpointer_references", DDS_view)) 

    {
        op_fail = FAIL;

        cons_out("\nThe internal all_backpointer_references view is not available for this -where_ref2 operation");
    }

    if( args->uid_flag && op_fail == OK )
    {
        /* Query the POM_BACKPOINTER and the PIMANRELATION tables */
        /* looking for where the target UID might be referenced   */

        std::vector< std::string >  tar_class;
        std::vector< int >          tar_cpid;
        std::string                 cur_class;
        int                         cur_cpid = -2;
        long                        ref_cnt = 0;

        std::stringstream sql;
        if (args->where_ref_sub == 1)
        {
            // Search the PIMANRELATION and POM_BACKPOINTER table for the specified UID.
            if( EIM_does_table_exist( "PIMANRELATION" ) == OK )
            {
                sql << "SELECT 'PIMANRELATION' AS Table_name, 'rsecondary_objectu' AS Col_Name, a.puid AS PUID, a.rsecondary_objectu AS Ref_UID, a.rsecondary_objectc AS Ref_CPID, b.pname AS Ref_Class, a.rprimary_objectc AS Target_CPID, c.pname AS Target_Class ";
                sql << "FROM PIMANRELATION a ";
                sql << "LEFT OUTER JOIN PPOM_CLASS b ON a.rsecondary_objectc = b.pcpid ";
                sql << "LEFT OUTER JOIN PPOM_CLASS c ON a.rprimary_objectc = c.pcpid ";
                sql << "WHERE rprimary_objectu = '" << args->uid << "' ";

                sql << "UNION ALL ";

                sql << "SELECT 'PIMANRELATION' AS Table_name, 'rprimary_objectu' AS Col_Name, a.puid AS PUID, a.rprimary_objectu AS Ref_UID, a.rprimary_objectc AS Ref_CPID, b.pname AS Ref_Class, a.rsecondary_objectc AS Target_CPID, c.pname AS Target_Class ";
                sql << "FROM PIMANRELATION a ";
                sql << "LEFT OUTER JOIN PPOM_CLASS b ON a.rprimary_objectc = b.pcpid ";
                sql << "LEFT OUTER JOIN PPOM_CLASS c ON a.rsecondary_objectc = c.pcpid ";
                sql << "WHERE rsecondary_objectu = '" << args->uid << "' ";

                sql << "UNION ALL ";
            }
            else
            {
                cons_out( "\nNo PIMANRELATION table was found, removing PIMANRELATION from the target tables." );
            }

            sql << "SELECT 'POM_BACKPOINTER' AS Table_name, 'from_uid' AS Col_Name, NULL AS PUID, a.from_uid AS Ref_UID, a.from_class AS Ref_CPID, b.pname AS Ref_Class, a.to_class AS Target_CPID, c.pname AS Target_Class ";
            sql << "FROM POM_BACKPOINTER a ";
            sql << "LEFT OUTER JOIN PPOM_CLASS b ON a.from_class = b.pcpid ";
            sql << "LEFT OUTER JOIN PPOM_CLASS c ON a.to_class = c.pcpid ";
            sql << "WHERE a.to_uid = '" << args->uid << "' ";

            sql << "UNION ALL ";

            sql << "SELECT 'POM_BACKPOINTER' AS Table_name, 'to_uid' AS Col_Name, NULL AS PUID, a.to_uid AS Ref_UID, a.to_class AS Ref_CPID, b.pname AS Ref_Class, a.from_class AS Target_CPID, c.pname AS Target_Class ";
            sql << "FROM POM_BACKPOINTER a ";
            sql << "LEFT OUTER JOIN PPOM_CLASS b ON a.to_class = b.pcpid ";
            sql << "LEFT OUTER JOIN PPOM_CLASS c ON a.from_class = c.pcpid ";
            sql << "WHERE a.from_uid = '" << args->uid << "' ";
        }
        else if (args->where_ref_sub == 2)
        {
            // Search the all_backpointer_references view for the specified UID.
            sql << "SELECT 'all_backpointer_references' AS Table_name, 'from_uid' AS Col_Name, NULL AS PUID, from_uid AS Ref_UID, -1 AS Ref_CPID, NULL AS Ref_Class, -1 AS Target_CPID, NULL AS Target_class FROM all_backpointer_references ";
            sql << "WHERE to_uid = '" << args->uid << "' ";

            sql << "UNION ALL ";
            sql << "SELECT 'all_backpointer_references' AS Table_name, 'to_uid' AS Col_Name, NULL AS PUID, to_uid AS Ref_UID, -1 AS Ref_CPID, NULL AS Ref_Class, -1 AS Target_CPID, NULL AS Target_class FROM all_backpointer_references ";
            sql << "WHERE from_uid = '" << args->uid << "' ";
        }


        EIM_value_p_t headers;
        EIM_row_p_t report;
        EIM_row_p_t row;
        EIM_select_var_t vars[8];
        EIM_select_col( &(vars[0]), EIM_varchar, "Table_Name",   CLS_DB_NAME_SIZE+1, false);
        EIM_select_col( &(vars[1]), EIM_varchar, "Col_Name",     ATT_DB_NAME_SIZE+1, false);
        EIM_select_col( &(vars[2]), EIM_varchar, "PUID",         MAX_UID_SIZE+1,      true);
        EIM_select_col( &(vars[3]), EIM_varchar, "Ref_UID",      MAX_UID_SIZE+1,     false);
        EIM_select_col( &(vars[4]), EIM_integer, "Ref_CPID",     sizeof(int),        false);
        EIM_select_col( &(vars[5]), EIM_varchar, "Ref_Class",    CLS_DB_NAME_SIZE+1, false);
        EIM_select_col( &(vars[6]), EIM_integer, "Target_CPID",  sizeof(int),         true);
        EIM_select_col( &(vars[7]), EIM_varchar, "Target_Class", CLS_DB_NAME_SIZE+1,  true);

        op_fail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, NULL, 8, vars, 0, NULL);

        if( report != NULL && op_fail == OK )
        {
            int col_size[WHERE_REF_COL_CNT] = { 6, 6, 6, 6, 4, 6 };
            int col_strt[WHERE_REF_COL_CNT] = { 0, 0, 0, 0, 0, 0 };
            const char* hdrs[WHERE_REF_COL_CNT] = { "Table", "Column", "PUID", "Ref_UID", "Ref_CPID", "Ref_Class" };

            for( row = report; row != NULL; row = row->next )
            {
                char *table_name = NULL;
                char *col_name = NULL;
                char *puid = NULL;
                char *ref_uid = NULL;
                int  *ref_cpid = NULL; 
                char *ref_class = NULL;
                int  *target_cpid = NULL;
                char *target_class = NULL;

                EIM_find_value (headers, row->line, "Table_Name",   EIM_varchar, (void **)&table_name);
                EIM_find_value (headers, row->line, "Col_Name",     EIM_varchar, (void **)&col_name);
                EIM_find_value (headers, row->line, "PUID",         EIM_varchar, (void **)&puid);
                EIM_find_value (headers, row->line, "Ref_UID",      EIM_varchar, (void **)&ref_uid);
                EIM_find_value (headers, row->line, "Ref_CPID",     EIM_integer, (void **)&ref_cpid);
                EIM_find_value (headers, row->line, "Ref_Class",    EIM_varchar, (void **)&ref_class);
                EIM_find_value (headers, row->line, "Target_CPID",  EIM_integer, (void **)&target_cpid);
                EIM_find_value (headers, row->line, "Target_Class", EIM_varchar, (void **)&target_class);

                if( table_name   != NULL && strlen( table_name )   > col_size[0] ) { col_size[0] = strlen( table_name ); }
                if( col_name     != NULL && strlen( col_name )     > col_size[1] ) { col_size[1] = strlen( col_name ); }
                if( puid         != NULL && strlen( puid )         > col_size[2] ) { col_size[2] = strlen( puid ); }
                if( ref_uid      != NULL && strlen( ref_uid )      > col_size[3] ) { col_size[3] = strlen( ref_uid ); }
                if( ref_class    != NULL && strlen( ref_class )    > col_size[5] ) { col_size[5] = strlen( ref_class ); }
 //             if( target_class != NULL && strlen( target_class ) > col_size[7] ) { col_size[7] = strlen( target_class ); }

                ref_cnt++;

                if( target_cpid != NULL && *target_cpid != cur_cpid )
                {
                    cur_cpid      = *target_cpid;
                    logical found = FALSE;

                    for( int i=0; i<tar_cpid.size(); i++ )
                    {
                        if( tar_cpid[i] == cur_cpid )
                        {
                            found = TRUE;
                            break;
                        }
                    }

                    if( !found )
                    {
                        tar_cpid.push_back( cur_cpid );

                        if( target_class != NULL )
                        {
                            tar_class.push_back( target_class );
                        }
                        else
                        {
                            tar_class.push_back( "<not found>" );
                        }
                    }
                }
            }

            // Calculate starting column postions
            for (int i=1; i<WHERE_REF_COL_CNT; i++ )
            {
                if( strlen( hdrs[i-1] ) < col_size[i-1] )
                {
                    col_strt[i] = col_strt[i-1] + col_size[i-1] + 2;
                }
                else
                {
                    col_strt[i] = col_strt[i-1] + strlen( hdrs[i-1] ) + 2;
                }
            }

            // Output target information
            if( tar_cpid.size() > 0 )
            {
                cons_out( "\nTARGET (UID  CPID  Class_Name):" );

                for( int i=0; i<tar_cpid.size(); i++ )
                {
                    std::stringstream msg;
                    msg.str("");
                    msg << args->uid << "  " << tar_cpid[i] << "  " << tar_class[i];
                    cons_out( msg.str() );
                }
            }
            else 
            {
                std::stringstream msg;
                msg.str("");
                msg << "\nTARGET: " << args->uid;
                cons_out( msg.str() );
            }

            // Output headers for referencing information
            std::stringstream msgx;
            msgx.str("");
            msgx << "\nREFERENCED FROM (cnt = " << ref_cnt << "):";
            cons_out( msgx.str() );

            {
                int pos = col_strt[0];
                std::stringstream msg;
                msg.str("");

                for( int i=0; i<WHERE_REF_COL_CNT; i++ )
                {
                    while( pos < col_strt[i] )
                    {
                        msg << " ";
                        pos++;
                    }

                    msg << hdrs[i];
                    pos += strlen( hdrs[i] );
                }
                cons_out( msg.str() );
            }


            // Output referencing data.
            for( row = report; row != NULL; row = row->next )
            {
                char *table_name = NULL;
                char *col_name = NULL;
                char *puid = NULL;
                char *ref_uid = NULL;
                int  *ref_cpid = NULL; 
                char *ref_class = NULL;
                int  *target_cpid = NULL;
                char *target_class = NULL;
                int  tmp = 0;

                EIM_find_value (headers, row->line, "Table_Name",   EIM_varchar, (void **)&table_name);
                EIM_find_value (headers, row->line, "Col_Name",     EIM_varchar, (void **)&col_name);
                EIM_find_value (headers, row->line, "PUID",         EIM_varchar, (void **)&puid);
                EIM_find_value (headers, row->line, "Ref_UID",      EIM_varchar, (void **)&ref_uid);
                EIM_find_value (headers, row->line, "Ref_CPID",     EIM_integer, (void **)&ref_cpid);
                EIM_find_value (headers, row->line, "Ref_Class",    EIM_varchar, (void **)&ref_class);
                EIM_find_value (headers, row->line, "Target_CPID",  EIM_integer, (void **)&target_cpid);
                EIM_find_value (headers, row->line, "Target_Class", EIM_varchar, (void **)&target_class);

                std::stringstream data_out;
                data_out.str("");

                // table_name
                tmp = 0;
                if( table_name != NULL )   
                { 
                    data_out << table_name; 
                    tmp = strlen( table_name ); 
                }

                while( tmp < col_strt[1] ) 
                { 
                    data_out << " "; 
                    tmp++;
                }

                // col_name
                tmp = 0;
                if( col_name != NULL )   
                { 
                    data_out << col_name; 
                    tmp = strlen( col_name ); 
                }

                while( (tmp + col_strt[1]) < col_strt[2] ) 
                { 
                    data_out << " ";
                    tmp++;
                }

                // puid
                tmp = 0;
                if( puid != NULL )   
                { 
                    data_out << puid; 
                    tmp = strlen( puid ); 
                }

                while( (tmp + col_strt[2]) < col_strt[3] ) 
                { 
                    data_out << " ";
                    tmp++;
                }

                // ref_uid
                tmp = 0;
                if( ref_uid != NULL )   
                { 
                    data_out << ref_uid; 
                    tmp = strlen( ref_uid ); 
                }

                while( (tmp + col_strt[3]) < col_strt[4] ) 
                { 
                    data_out << " ";
                    tmp++;
                }

                // ref_cpid
                tmp = 0;
                if( ref_cpid != NULL )   
                { 
                    std::stringstream tmp_str;
                    tmp_str << *ref_cpid; 

                    data_out << *ref_cpid; 
                    tmp = tmp_str.str().length(); 
                }

                while( (tmp + col_strt[4]) < col_strt[5] ) 
                { 
                    data_out << " ";
                    tmp++;
                }
           
                // ref_class
                tmp = 0;
                if( ref_class != NULL )   
                { 
                    data_out << ref_class; 
                    tmp = strlen( ref_class ); 
                }
/*
                while( (tmp + col_strt[5]) < col_strt[6] ) 
                { 
                    data_out << " ";
                    tmp++;
                }

                // target_cpid
                tmp = 0;
                if( target_cpid != NULL )   
                { 
                    std::stringstream tmp_str;
                    tmp_str << *target_cpid; 

                    data_out << *target_cpid; 
                    tmp = tmp_str.str().length(); 
                }

                while( (tmp + col_strt[6]) < col_strt[7] ) 
                { 
                    data_out << " ";
                    tmp++;
                }
           
                // target_class
                tmp = 0;
                if( target_class != NULL )   
                { 
                    data_out << target_class; 
                    tmp = strlen( target_class ); 
                }
*/
                cons_out( data_out.str() );
            }

            EIM_free_result( headers, report);
        }
        else
        {
            std::stringstream msg;
            if( args->where_ref_sub == 1)  msg << "\nThe target object (" << args->uid << ") was NOT found in POM_BACKPOINTER or in PIMANRELATION.";
            if( args->where_ref_sub == 2)  msg << "\nThe target object (" << args->uid << ") was NOT found in all_backpointer_references view.";
            cons_out( msg.str() );
        }

        if( op_fail != OK )
        {
            std::stringstream msg;
            if( args->where_ref_sub == 1)  msg << "\nUnable to access PIMANRELATION table or POM_BACKPOINTER table, ifail = " << op_fail;
            if( args->where_ref_sub == 2)  msg << "\nUnable to access all_backpointer_references view, ifail = " << op_fail;
            cons_out( msg.str() );
        }
     }  
    else 
    {
        if (op_fail == OK)
        {
            cons_out("\nThe -uid=<uid> option is required with the -where_ref and -where_ref2 options");
        }
        
        op_fail = FAIL; 
    }

    return( op_fail );
}

static int str_len_val_op( int* bad_count)
{
    int op_fail = OK;
    int ifail = OK;
    std::string tar_class;
    std::string tar_attribute;
    std::string tar_uid;
    tag_t       tar_class_tag = NULL_TAG;
    tag_t       tar_attr_tag = NULL_TAG;
    int         tar_desc = 0;
    int         tar_type = 0;    // DDS_long_string, DDS_string, etc...
    int         tar_slen = 0;    // Attribute maximum string length  
    int         tar_arry = 0;    // Attribute array size, -1 = VLA, 1-6 = Small array, 7+ = Large array.
    int         tar_pos = -1;
    logical     work_done = FALSE;
    *bad_count = 0;

    if ( EIM_dbplat() == EIM_dbplat_postgres || ( EIM_dbplat() == EIM_dbplat_oracle && EIM_get_db_cs() == TEXT_CODESET_UTF8 ) )
    {
        std::stringstream msg;
        msg << "The -str_len_val options is only avaliable with the MS SQL Server, or with Oracle that is NOT configured for UTF-8"; 
        op_fail = error_out(ERROR_line, POM_op_not_supported, msg.str());
    }

    if (op_fail != OK)
    {
        return(op_fail);
    }

    if ( EIM_dbplat() == EIM_dbplat_oracle && args->expected_error == POM_op_not_supported)
    {
        // if this feature, in this configurationa, is actually supported, then change expected-error of POM_op_not_supported to POM_ok.
        args->expected_error = POM_ok;   
    }

    // Parse the "from" arguments
    if (!args->from_flag)
    {
        op_fail = error_out(ERROR_line, POM_invalid_value, "-str_len_val option requires the \"-from=class:attribute\" parameter ");
    }
    else
    {
        tar_class = getSubParameter(args->from, 0);

        if (tar_class.empty())
        {
            op_fail = error_out(ERROR_line, POM_invalid_value, "The \"-from=...\" option requires a class subparameter - E.g. \"-from=class:attribute\"");
        }

        tar_attribute = getSubParameter(args->from, 1);

        if (tar_attribute.empty())
        {
            op_fail = error_out(ERROR_line, POM_invalid_value, "The \"-from=...\" option requires an attribute subparameter - E.g. \"-from=class:attribute\"");
        }

        tar_uid = getSubParameter(args->from, 2);

        const std::string tar_pos_optional_val = getSubParameter(args->from, 3);
        if (!tar_pos_optional_val.empty())
        {
            std::istringstream tar_pos_stream(tar_pos_optional_val);
            tar_pos_stream >> tar_pos;
        }
    }

    // Check the "from" class.
    ifail = POM_class_id_of_class(tar_class.c_str(), &tar_class_tag);

    if (ifail != OK)
    {
        std::stringstream msg;
        msg << "The \"from\" class (" << tar_class << ") is invalid";
        op_fail = error_out(ERROR_line, ifail, msg.str());
    }
    else
    {
        // validate attribute here.
        ifail = POM_attr_id_of_attr(tar_attribute.c_str(), tar_class.c_str(), &tar_attr_tag);

        if (ifail != OK)
        {
            std::stringstream msg;
            msg << "The \"from\" class (" << tar_class << ") does not contain an attribute named \"" << tar_attribute << "\"";
            op_fail = error_out(ERROR_line, ifail, msg.str());
        }
        else
        {
            char** lcl_names = NULL;
            int*   lcl_types = NULL;
            int*   lcl_str_lengths = NULL;
            tag_t* lcl_ref_classes = NULL;
            int*   lcl_lengths = NULL;
            int*   lcl_descr = NULL;
            int*   lcl_failures = NULL;

            ifail = POM_describe_attrs(tar_class_tag, 1, &tar_attr_tag, &lcl_names, &lcl_types, &lcl_str_lengths, &lcl_ref_classes, &lcl_lengths, &lcl_descr, &lcl_failures);

            if (ifail != OK)
            {
                std::stringstream msg;
                msg << "Unable to describe attribute " << tar_class << "." << tar_attribute;
                op_fail = error_out(ERROR_line, ifail, msg.str());
            }
            else
            {
                tar_desc = *lcl_descr;
                tar_type = *lcl_types;
                tar_arry = *lcl_lengths;
                tar_slen = *lcl_str_lengths;

                SM_free((void *)lcl_names);
                SM_free((void *)lcl_types);
                SM_free((void *)lcl_str_lengths);
                SM_free((void *)lcl_ref_classes);
                SM_free((void *)lcl_lengths);
                SM_free((void *)lcl_descr);
                SM_free((void *)lcl_failures);
            }
        }
    }

    if (tar_type != POM_long_string && tar_type != POM_string)
    {
        std::stringstream msg;
        msg << "The \"from\" attribute (" << tar_attribute << ") must be a string or long-string attribute";
        op_fail = error_out(ERROR_line, POM_invalid_attr_id, msg.str());
    }

    if (op_fail != OK)
    {
        return(op_fail);
    }

    const char* cls_tbl = NULL;
    const char* cls_col = NULL;
    const char* att_tbl = NULL;
    const char* att_col = NULL;

    op_fail = get_tables_and_columns(tar_class.c_str(), tar_attribute.c_str(), &cls_tbl, &cls_col, &att_tbl, &att_col);

    if (op_fail != OK)
    {
        std::stringstream msg;
        msg << "Unable to find tables and columns for class " << tar_class << " and attribute " << tar_attribute << " using get_tables_and_columns()";
        op_fail = error_out(ERROR_line, op_fail, msg.str());
        return(op_fail);
    }

    op_fail = database_tx_check();

    if (op_fail != OK)
    {
        return(op_fail);
    }

    START_WORKING_TX(str_len_val_tx, "str_len_val_tx");

    ERROR_PROTECT

    if (tar_arry == 1 || tar_arry == -1 || tar_arry > 6 || tar_arry <= 6)
    {
        std::vector< std::string >  puids;
        std::vector< int > pseqs;
        std::vector< int > calc_sizes;
        std::vector< int > stored_sizes;

        const char* uid = NULL;

        if (!tar_uid.empty())
        {
            uid = tar_uid.c_str();
        }

        switch (tar_type)
        {
        case POM_long_string:
            if (tar_arry == 1)
            {
                // POM_long_string array size = 1
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by calc_sizes in assending order.
                ifail = get_long_string_sizes(cls_tbl, cls_col, att_tbl, att_col, uid, args->uid_vec, puids, calc_sizes, stored_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve long-string sizes from database - get_long_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // Now validate the stored string sizes with the data's actual length. 
                    // If -commit was specified on the command line then update the stored string sizes.
                    ifail = validate_long_string_sizes("LSt", tar_class.c_str(), tar_attribute.c_str(), cls_tbl, cls_col, att_tbl, att_col, puids, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate long-string sizes in database - validate_long_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo records were found in the database,";
                        cons_out(msg.str());
                    }
                }
            }
            else if(tar_arry == -1)
            {
                // POM_long_string array size = -1
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by puid, seq, calc_sizes in assending order.
                ifail = get_vla_long_string_sizes(att_tbl, att_col, uid, args->uid_vec, puids, pseqs, calc_sizes, stored_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve VLA long-string sizes from database - get_vla_long_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // No validate the stored string sizes with the data's actual length. 
                    // If -commit was specified on the command line then update the stored string sizes.
                    ifail = validate_vla_long_string_sizes("LSt(VLA)", tar_class.c_str(), tar_attribute.c_str(), att_tbl, att_col, puids, pseqs, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate long-string sizes in database - validate_long_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo records were found in the database,";
                        cons_out(msg.str());
                    }
                }
            }
            else if (tar_arry > 6)
            {
                // POM_long_string array size > 6
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by puid, seq, calc_sizes in assending order.
                ifail = get_la_long_string_sizes(att_tbl, att_col, uid, args->uid_vec, puids, pseqs, calc_sizes, stored_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve (LA) large-array long-string sizes from database - get_la_long_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // No validate the stored string sizes with the data's actual length. 
                    // If -commit was specified on the command line then update the stored string sizes.
                    ifail = validate_la_long_string_sizes(tar_class.c_str(), tar_attribute.c_str(), att_tbl, att_col, puids, pseqs, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate (LA) large-array long-string sizes in database - validate_la_long_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo records were found in the database,";
                        cons_out(msg.str());
                    }
                }
            }
            else if (tar_arry <= 6)
            {
                // POM_long_string array size <= 6
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by puid, seq, calc_sizes in assending order.
                ifail = get_sa_long_string_sizes(att_tbl, att_col, uid, args->uid_vec, puids, pseqs, calc_sizes, stored_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve (SA) small-array long-string sizes from database - get_sa_long_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // No validate the stored string sizes with the data's actual length. 
                    // If -commit was specified on the command line then update the stored string sizes.
                    ifail = validate_sa_long_string_sizes(tar_class.c_str(), tar_attribute.c_str(), att_tbl, att_col, puids, pseqs, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate (SA) small-array long-string sizes in database - validate_sa_long_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo records were found in the database,";
                        cons_out(msg.str());
                    }
                }
            }
            else
            {
                ERROR_raise(ERROR_line, POM_op_not_supported, "Unsupported array size for a POM-long-string attribute.");
            }
            break;

        case POM_string:
            if (tar_arry == 1)
            {
                // POM_string array size = 1
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by calc_sizes in assending order.
                ifail = get_string_sizes(cls_tbl, cls_col, tar_slen, uid, args->uid_vec, puids, calc_sizes);


                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve long-string sizes from database - get_long_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // Now validate the strings sizes of POM_string_attributes
                    ifail = validate_string_sizes("Str", tar_class.c_str(), tar_attribute.c_str(), cls_tbl, cls_col, tar_slen, puids, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate string sizes in database - validate_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo database records found that met or exceeded the maximum size";
                        cons_out(msg.str());
                    }
                }
            }
            else if (tar_arry == -1)
            {
                // POM_string array size = -1
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by puid, seq, calc_sizes in assending order.
                ifail = get_vla_string_sizes(att_tbl, att_col, tar_slen, uid, args->uid_vec, puids, pseqs, calc_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve VLA-string sizes from database - get_vla_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // Now validate the strings sizes of POM_string_attributes
                    ifail = validate_vla_string_sizes("Str(VLA)", tar_class.c_str(), tar_attribute.c_str(), att_tbl, att_col, tar_slen, puids, pseqs, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate VLA string sizes in database - validate_vla_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo database records found that met or exceeded the maximum size";
                        cons_out(msg.str());
                    }
                }
            }
            else if (tar_arry > 6)
            {
                // POM_string array size > 6 (large-array)
                // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                // Returned values are ordered by puid, seq, calc_sizes.
                ifail = get_la_string_sizes(att_tbl, att_col, tar_slen, uid, args->uid_vec, puids, pseqs, calc_sizes);

                if (ifail != OK)
                {
                    std::stringstream msg;
                    msg << "Unable to retrieve (LA) large-array string sizes from database - get_la_string_sizes() returned " << ifail << ".";
                    op_fail = error_out(ERROR_line, ifail, msg.str());
                }

                if (op_fail == OK && puids.size() > 0)
                {
                    // Now validate the strings sizes of POM_string_attributes
                    ifail = validate_la_string_sizes(tar_class.c_str(), tar_attribute.c_str(), att_tbl, att_col, tar_slen, puids, pseqs, calc_sizes, bad_count);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to validate (LA) larve-array string sizes in database - validate_la_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }
                    else
                    {
                        work_done = true;
                    }
                }
                else
                {
                    if (op_fail == OK && puids.size() == 0)
                    {
                        std::stringstream msg;
                        msg << "\nNo database records found that met or exceeded the maximum size";
                        cons_out(msg.str());
                    }
                }
            }
            else if (tar_arry <= 6)
            {
                const char* col_name = NULL;
                const char* func = NULL;

                for (int i = 0; i < tar_arry; i++)
                {
                    puids.clear();
                    calc_sizes.clear();
                    SM_free((void*)col_name);
                    SM_free((void*)func);

                    col_name = SM_sprintf("%s_%d", cls_col, i);
                    func = SM_sprintf("Str(SA)%d", i);

                    // POM_string array size <= 6 (Small array)
                    // Get the calculated sizes (datalength(X)) and the stored sizes from the database. 
                    // Returned values are ordered by calc_sizes in assending order.
                    ifail = get_string_sizes(cls_tbl, col_name, tar_slen, uid, args->uid_vec, puids, calc_sizes);

                    if (ifail != OK)
                    {
                        std::stringstream msg;
                        msg << "Unable to retrieve small-array string sizes from database - get_string_sizes() returned " << ifail << ".";
                        op_fail = error_out(ERROR_line, ifail, msg.str());
                    }

                    if (op_fail == OK && puids.size() > 0)
                    {
                        // Now validate the strings sizes of POM_string_attributes
                        ifail = validate_string_sizes(func, tar_class.c_str(), tar_attribute.c_str(), cls_tbl, col_name, tar_slen, puids, calc_sizes, bad_count);

                        if (ifail != OK)
                        {
                            std::stringstream msg;
                            msg << "Unable to validate string sizes in database - validate_string_sizes() returned " << ifail << ".";
                            op_fail = error_out(ERROR_line, ifail, msg.str());
                        }
                        else
                        {
                            work_done = true;
                        }
                    }
                    else
                    {
                        if (op_fail == OK && puids.size() == 0)
                        {
                            std::stringstream msg;
                            msg << "      " << func << ": No database records found that met or exceeded the maximum size";
                            cons_out(msg.str());
                        }
                    }

                    if (op_fail != OK)
                    {
                        break;
                    }
                }
                SM_free((void*)col_name);
                SM_free((void*)func);
            }
            else
            {
                ERROR_raise(ERROR_line, POM_op_not_supported, "Unsupported array size for a POM-string attribute.");
            }
            break;
        default:
            ERROR_raise(ERROR_line, POM_internal_error, "Unsupport attribute type detected in str_len_val_op() - Only POM_string and POM_long_string are supported.");
        }
    }
    else
    {
        // Unknown array length
        std::stringstream msg;
        msg << "Currenly only scaler long-string and string attributes are supported, an array size of  " << tar_arry << " is not supported.";
        op_fail = error_out(ERROR_line, POM_op_not_supported, msg.str());
    }

    ERROR_RECOVER

    if (op_fail == OK)
    {
        op_fail = ERROR_ask_failure_code();
    }
    ERROR_END

    SM_free((void*)cls_tbl);
    SM_free((void*)cls_col);
    SM_free((void*)att_tbl);
    SM_free((void*)att_col);

    // Commit the work only if it was successful and the user said to commit on the command line.
    if ((op_fail == OK) && args->commit_flag && work_done)
    {
        COMMIT_WORKING_TX(str_len_val_tx, "str_len_val_tx");
    }
    else
    {
        ROLLBACK_WORKING_TX(str_len_val_tx, "str_len_val_tx");
    }

    return(op_fail);
}

static int str_len_meta_op()
{
    int op_fail = OK;
    std::string tar_class;

    if (!args->class_flag)
    {
        std::stringstream msg;
        msg << "The -str_len_meta options requires a class name (-c=name).";
        op_fail = error_out(ERROR_line, POM_invalid_value, msg.str());
    }

    const char *superclass = NULL;

    op_fail = get_string_meta(args->class_n, &superclass);

    while (superclass != NULL && op_fail == OK)
    {
        const char *tar_class = superclass;
        superclass = NULL;

        op_fail = get_string_meta(tar_class, &superclass);

        SM_free((void*)tar_class);
    }

    return(op_fail);
}

static logical compare_object_to_bp( const std::string from_uid, const std::string from_class, const std::string to_uid, const std::string to_class, int refs, std::vector< bp_t > &bptrs )
{
    logical ret_val = TRUE;  // TRUE = object and backpointers are the same, FALSE = backpointers should be adjusted to match the object.

    if( refs == 0 && bptrs.size() == 0 )
    {
        return( ret_val );
    }

    ret_val = FALSE;

    if( bptrs.size() > 1 || (refs == 0 && bptrs.size() > 0) || (refs > 0 && bptrs.size() == 0) )
    {
        return( ret_val );
    }

    bp_t *bptr = &(bptrs[0]);

    if( from_uid.compare( bptr->from_uid ) != 0 ) return( ret_val );

    if( from_class.compare( bptr->from_class_name ) != 0 ) return( ret_val );

    if( to_uid.compare( bptr->to_uid ) != 0 ) return( ret_val );

    if( to_class.compare( bptr->to_class_name )  != 0 ) return( ret_val );

    if( refs != bptr->bp_count ) return( ret_val );


    ret_val = TRUE;
    return( ret_val );
}

/* *************************************** */
/* VLA scanning functionality starts here. */
/* *************************************** */
static int scan_vla_op( int *found_count )
{
    int ifail = OK;

    // Validate input paramters.
    ifail = validate_cmd_line_class_and_attribute_params( );

    if ( args->uid_flag )
    {
        std::vector< std::string >* uid_vec_lcl = args->uid_vec;
        int uid_cnt = (*uid_vec_lcl).size( );

        if ( uid_cnt > 100 )
        {
            ifail = POM_invalid_value;
            std::stringstream msg;
            msg << "\nError " << ifail << " A maximum of 100 UIDs can be specified.";
            cons_out( msg.str() );
        }
    }
    if ( ifail != OK )
    {
        return ifail;
    }

    /* Get class hierarchy */
    std::vector<hier_t> hier;
    getHierarchy( hier );

    /* Get system metadata of all VLAs */
    std::vector<cls_t> meta;
    getVlaMeta( meta, hier );

    if ( args->debug_flag )
    {
        dumpHierarchy( hier );
        dumpRefMetadata( meta );
    }

    int vla_cnt = 0;
    int class_cnt = meta.size();
    int att_processed = 0;
    int flat_att_processed = 0;
    char* last_class = NULL;
    char* last_att = NULL;
    int lcl_ifail = OK;

    /* Loop through each class and each VLA      */
    /* attribute checking for inconsistent VLAs. */
    for ( int i = 0; i < class_cnt; i++ )
    {
        last_class = meta[i].name;

        if ( args->class_flag && strcmp( last_class, args->class_n ) != 0 )
        {
            continue;
        }

        // Search normal class attributes and output the information.
        lcl_ifail = output_vlas( &meta[i], &vla_cnt, &att_processed, &last_att );

        if ( ifail == OK && lcl_ifail != OK )
        {
            ifail = lcl_ifail;
        }

        // Search flattened attributes and output the information.
        lcl_ifail = output_flattened_vlas( hier, meta, &meta[i], &vla_cnt, &flat_att_processed );

        if ( ifail == OK && lcl_ifail != OK )
        {
            ifail = lcl_ifail;
        }
    }

    if ( last_class != NULL && last_att != NULL )
    {
        std::stringstream msg;
        msg << "\nLast attribute processed is " << last_class << ":" << last_att;
        cons_out( msg.str() );
    }

    *found_count = vla_cnt;
    std::stringstream msg;
    msg << "\nTotal system VLA attributes        = " << args->att_cnt;
    msg << "\nNormal VLA attributes processed    = " << att_processed;
    msg << "\nFlattened VLA attributes processed = " << flat_att_processed;
    msg << "\nTotal inconsistent VLAs found      = " << vla_cnt;      
    cons_out( msg.str() );

    return ( ifail );
}

#define VSR_HDR_LINE '*'
#define VSR_DATA_LINE '.'
#define VSR_INFO_LINE '+'

/*-----------------------------------------------------------------*/
static void output_scan_vla_err( const cls_t* cls, const cls_t* flat, const att_t* att, int ifail, const char* msg )
{
    std::stringstream out_msg;

    if ( flat != NULL )
    {
        out_msg << " ERROR " << flat->name << "\\" << cls->name << ":" << att->name;
        out_msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att );
    }
    else
    {
        out_msg << " ERROR " << cls->name << ":" << att->name;
        out_msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att );
    }

    out_msg << "): ifail = " << ifail;

    if ( msg != NULL )
    {
        out_msg << " - " << msg;
    }

    cons_out( out_msg.str() );
}

/*-----------------------------------------------------------------*/
static void output_scan_vla_msg( const cls_t* cls, const cls_t* flat, const att_t* att, const char* msg )
{
    std::stringstream out_msg;

    if ( flat != NULL )
    {
        out_msg << " Flat  " << flat->name << "\\" << cls->name << ":" << att->name;
        out_msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att );
    }
    else
    {
        out_msg << "       " << cls->name << ":" << att->name;
        out_msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att );
    }

    out_msg << ") - " << msg;
    cons_out( out_msg.str() );
}

/*-----------------------------------------------------------------*/
static void output_scan_vla_attr_hdr( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att, int uid_cnt )
{
    std::stringstream msg;

    if ( prefix == VSR_DATA_LINE )
    {
        msg << "\n" << VSR_HDR_LINE;
    }
    else
    {
        msg << "\n" << prefix;
    }

    std::string storage_mode = get_storage_mode( flat != NULL ? flat->name : cls->name );

    if ( flat != NULL )
    {
        msg << "  " << storage_mode << " " << flat->name << "\\" << cls->name << ":" << att->name << "[" << att->pptype << "]";
        msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att ) << "): ";
    }
    else
    {
        msg << "  " << storage_mode << " " << cls->name << ":" << att->name << "[" << att->pptype << "]";
        msg << " (" << get_vla_class_table( cls, flat ) << "." << get_vla_count_column( cls, att ) << ":" << get_vla_table( att ) << "): ";
    }

    if ( prefix == VSR_DATA_LINE )
    {
        if ( uid_cnt >= 0 )
        {
            msg << att->uid_cnt << " inconsistent VLAs found";
        }

        if ( !args->min_flag )
        {
            msg << "\n" << VSR_HDR_LINE << "  uid,class-cnt,sequence";
        }
    }

    cons_out( msg.str( ) );
}

/*---------------------------------------------------------------------*/
/* Retrieves the class count and all sequence values for a spcific UID */
static void output_scan_vla_details( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att )
{
    logical trans_was_active = true;
    logical header_output = false;

    if ( !EIM_is_transaction_active( ) )
    {
        trans_was_active = false;
        EIM_start_transaction( );
    }

    for ( int i = 0; i < att->uid_cnt; i++ )
    {
        int ifail = OK;
        char* uid = att->uids[i].uid;
        EIM_select_var_t vars[4];
        EIM_value_p_t headers = NULL;
        EIM_row_p_t report = NULL;
        EIM_row_p_t row;
        int row_cnt = 0;

        ERROR_PROTECT
        std::stringstream sql;

        sql << "SELECT t1.puid, t1." << get_vla_count_column( cls, att ) << " AS cnt, t2.puid AS puid2, t2.pseq FROM " << get_vla_class_table( cls, flat ) << " t1 LEFT OUTER JOIN ";
        sql << get_vla_table( att ) << " t2 ON t1.puid = t2.puid WHERE t1.puid = :1 ORDER BY t2.pseq";

        EIM_select_col( &(vars[0]), EIM_puid, "puid", MAX_UID_SIZE, false );
        EIM_select_col( &(vars[1]), EIM_integer, "cnt", sizeof( int ), false );
        EIM_select_col( &(vars[2]), EIM_puid, "puid2", MAX_UID_SIZE, true );
        EIM_select_col( &(vars[3]), EIM_integer, "pseq", sizeof( int ), true );

        EIM_bind_var_t bindVars[1];
        EIM_bind_val( &bindVars[0], EIM_puid, sizeof( EIM_uid_t ), uid );
        ifail = EIM_exec_sql_bind( sql.str( ).c_str( ), &headers, &report, 0, 4, vars, 1, bindVars );

        if ( !args->ignore_errors_flag )
        {
            EIM_check_error( "output_scan_vla_details()\n" );
        }
        else if ( ifail != OK )
        {
            EIM_clear_error( );
            EIM_free_result( headers, report );
            report = NULL;
            headers = NULL;

            if ( !header_output )
            {
                output_scan_vla_attr_hdr( prefix, cls, flat, att, att->uid_cnt );
                header_output = true;
            }

            std::stringstream msg;
            msg << prefix << "  " << uid << ",-1,<<< Error retrieving VLA data from the database. (ifail = " << ifail << ") >>>";
            cons_out( msg.str( ) );
        }

        if ( report != NULL )
        {
            row_cnt = 0;

            for ( row = report; row != NULL; row = row->next ) row_cnt++;

            if ( row_cnt > 0 )
            {
                row = report;

                if ( !header_output )
                {
                    output_scan_vla_attr_hdr( prefix, cls, flat, att, att->uid_cnt );
                    header_output = true;
                }

                // Get the count from the class table.
                int* cls_cnt = NULL;
                EIM_find_value( headers, row->line, "cnt", EIM_integer, &cls_cnt );

                if ( cls_cnt == NULL )
                {
                    std::stringstream msg;
                    msg << prefix << "  " << uid << ",-1,<<< ERROR Count column is NULL >>>";
                    cons_out( msg.str( ) );
                }
                else
                {
                    std::stringstream msg;
                    msg << prefix << "  " << uid << "," << *cls_cnt << ",";

                    // Get the sequence values
                    int first_valid = -1;
                    int last_valid = -1;

                    for ( row = report; row != NULL; row = row->next )
                    {
                        int *int_ptr = NULL;
                        EIM_find_value( headers, row->line, "pseq", EIM_integer, &int_ptr );

                        if ( int_ptr != NULL )
                        {
                            if ( first_valid == -1 )
                            {
                                first_valid = *int_ptr;
                                last_valid = *int_ptr;
                            }
                            else if ( (last_valid + 1) == *int_ptr )
                            {
                                last_valid = *int_ptr;
                            }
                            else if ( first_valid == last_valid )
                            {
                                msg << first_valid << " ";
                                first_valid = *int_ptr;
                                last_valid = *int_ptr;
                            }
                            else
                            {
                                msg << first_valid << "-" << last_valid << " ";
                                first_valid = *int_ptr;
                                last_valid = *int_ptr;
                            }
                        }
                    }

                    if ( first_valid != -1 )
                    {
                        if ( first_valid == last_valid )
                        {
                            msg << first_valid << " ";
                        }
                        else
                        {
                            msg << first_valid << "-" << last_valid << " ";
                        }
                    }

                    cons_out( msg.str( ) );
                    first_valid = -1;
                    last_valid = -1;
                }
            }

            EIM_free_result( headers, report );
            report = NULL;
            headers = NULL;
        }

        ERROR_RECOVER
        const std::string msg( "EXCEPTION: See syslog for additional details. (See -i option to ignore this error.)" );
        cons_out( msg );

        if ( !trans_was_active )
        {
            EIM__clear_transaction( ERROR_ask_failure_code( ) );
            ERROR_raise( ERROR_line, EIM_ask_abort_code( ), "Failed to execute the query\n" );
        }
        else
        {
            ERROR_reraise( );
        }
        ERROR_END
    }

    if ( !trans_was_active )
    {
        EIM_commit_transaction( "output_scan_vla_details()" );
    }
}

/*------------------------------------------------------------------------*/
static void output_scan_vla_parallel_data( const cls_t* cls, const cls_t* flat, att_t* attr )
{
    int att_cnt = cls->att_cnt;
    int att_id = attr->att_id;

    for ( int i = 0; i < att_cnt; i++ )
    {
        att_t* att = &cls->atts[i];

        if ( att->att_id == att_id )
        {
            continue;
        }

        att->uid_cnt = attr->uid_cnt;
        att->uids = attr->uids;

        output_scan_vla_details( VSR_INFO_LINE, cls, flat, att );

        att->uid_cnt = 0;
        att->uids = NULL;
    }
}

/*-----------------------------------------------------------------*/
static int get_inconsistent_vlas( cls_t* cls, const cls_t* flat, att_t* att )
{
    int ifail = OK;
    logical trans_was_active = true;
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    int row_cnt = 0;
    ref_t* alloc_uids = NULL;

    ERROR_PROTECT
    if ( !EIM_is_transaction_active() )
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    std::stringstream sql;

    switch ( EIM_dbplat() )
    {
    case EIM_dbplat_mssql:
        sql << "SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " != (SELECT ISNULL(COUNT(t2.puid), 0) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    case EIM_dbplat_oracle:
        sql << "SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " != (SELECT NVL(COUNT(t2.puid), 0) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    case EIM_dbplat_postgres:
        sql << "SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " != (SELECT COUNT(t2.puid) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    default:
        ERROR_raise( ERROR_line, POM_internal_error, "Unsupported database platform." );
        break;
    }

    if ( args->uid_flag )
    {
        std::vector< std::string >* uid_vec_lcl = args->uid_vec;
        int uid_cnt = (*uid_vec_lcl).size( );

        if ( uid_cnt == 1 )
        {
            sql << " AND t1.puid = '" << (*uid_vec_lcl)[0] << "'";
        }
        else if ( uid_cnt > 1 )
        {
            for ( int i = 0; i < uid_cnt; i++ )
            {
                if ( i == 0 )
                {
                    sql << " AND t1.puid IN (";
                }
                else
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec_lcl)[i] << "'";
            }
            sql << ")";
        }
    }

    switch ( EIM_dbplat( ) )
    {
    case EIM_dbplat_mssql:
        sql << " UNION";
        sql << " SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " > 0 AND (((CAST(" << get_vla_count_column( cls, att ) << " AS bigint))*(CAST(" << get_vla_count_column( cls, att ) << " AS bigint) - 1))/2) !=";
        sql << " (SELECT ISNULL(SUM(CAST(t2." << get_vla_seq_column( att ) << " as bigint)), 0) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    case EIM_dbplat_oracle:
        sql << " UNION";
        sql << " SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " > 0 AND ((" << get_vla_count_column( cls, att ) << " * (" << get_vla_count_column( cls, att ) << " - 1))/2) !=";
        sql << " (SELECT NVL(SUM(t2." << get_vla_seq_column( att ) << "), 0) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    case EIM_dbplat_postgres:
        sql << " UNION";
        sql << " SELECT puid FROM " << get_vla_class_table( cls, flat ) << " t1 WHERE t1." << get_vla_count_column( cls, att );
        sql << " > 0 AND ((t1." << get_vla_count_column( cls, att ) << " * (t1." << get_vla_count_column( cls, att ) << " - 1))/2) !=";
        sql << " (SELECT COALESCE(SUM(t2." << get_vla_seq_column( att ) << "), 0) FROM " << get_vla_table( att ) << " t2 WHERE t1.puid = t2.puid)";
        break;

    default:
        ERROR_raise( ERROR_line, POM_internal_error, "Unsupported database platform." );
        break;
    }

    if ( args->uid_flag )
    {
        std::vector< std::string >* uid_vec_lcl = args->uid_vec;
        int uid_cnt = (*uid_vec_lcl).size( );

        if ( uid_cnt == 1 )
        {
            sql << " AND t1.puid = '" << (*uid_vec_lcl)[0] << "'";
        }
        else if ( uid_cnt > 1 )
        {
            for ( int i = 0; i < uid_cnt; i++ )
            {
                if ( i == 0 )
                {
                    sql << " AND t1.puid IN (";
                }
                else
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec_lcl)[i] << "'";
            }
            sql << ")";
        }
    }

    EIM_select_col( &( vars[0] ), EIM_varchar, "puid", MAX_UID_SIZE, false );
    ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if ( !args->ignore_errors_flag )
    {
        EIM_check_error( "get_inconsistent_vlas()\n" );
    }
    else if ( ifail != OK )
    {
        EIM_clear_error();

        std::stringstream msg;
        msg << "\nError " << ifail << " while reading " << get_vla_table( att );

        if ( flat == NULL )
        {
            msg << ". SKIPPING class:attribute " << cls->name << ":" << att->name << ".\n";
        }
        else
        {
            msg << ". SKIPPING flat_class\\class:attribute " <<flat->name << "\\" << cls->name << ":" << att->name << ".\n";
        }
        cons_out( msg.str() );

        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    if ( report != NULL )
    {
        row_cnt = 0;

        for ( row = report; row != NULL; row = row->next )
            row_cnt++;

        alloc_uids = (ref_t*)SM_calloc_persistent( row_cnt, sizeof( ref_t ) );

        int i = 0;

        for ( row = report; row != NULL; row = row->next )
        {
            char* tmp = NULL;
            EIM_find_value( headers, row->line, "puid", EIM_varchar, &tmp );
            strncpy( alloc_uids[i].uid, tmp, MAX_UID_SIZE );
            alloc_uids[i].uid[MAX_UID_SIZE] = '\0';
            i++;
        }

        att->uids = alloc_uids;
        att->uid_cnt = row_cnt;

        if ( flat == NULL )
        {
            cls->ref_cnt += row_cnt;
        }
        else
        {
            cls->flt_cnt += row_cnt;
        }
        EIM_free_result( headers, report );
    }

    if ( !trans_was_active )
    {
        EIM_commit_transaction( "get_inconsistent_vlas()" );
    }

    ERROR_RECOVER
    const std::string msg( "EXCEPTION: See syslog for additional details. (See -i option to ignore this error.)" );
    cons_out( msg );

    if ( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n" );
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return ( ifail );
}

/*------------------------------------------------------------------------
** Searches flattened VLA attributes for inconsistent VLAs.
** ----------------------------------------------------------------------- */
static int output_flattened_vlas( const std::vector<hier_t>& hier, std::vector<cls_t>& meta, cls_t* flat, int* accum_cnt, int* attr_cnt )
{
    int ifail = OK;

    if ( isFlat( flat ) )
    {
        // Process the attributes that have been flattened into this class
        cls_t* cur_cls = NULL;
        int cur_cpid = flat->cls_id;
        int par_cpid = hier[cur_cpid].par_id;

        // Walk up the hiearchy until we come to POM_object (cpid = 1)

        while ( par_cpid > 0 && hier[par_cpid].cls_pos >= 0 )
        {
            int pos = hier[par_cpid].cls_pos;
            cur_cls = &meta[pos];
            cur_cpid = par_cpid;
            par_cpid = hier[cur_cpid].par_id;

            int att_cnt = cur_cls->att_cnt;

            for ( int j = 0; j < att_cnt; j++ )
            {
                // Skip everything except VLA Attributes.
                if ( !isVLA( &cur_cls->atts[j] ) )
                {
                    continue;
                }

                // Looking for a flattened attribute with a specific name?
                if ( args->attribute_flag )
                { 
                    if( strcasecmp( cur_cls->atts[j].name, args->attribute ) != 0 )
                    {
                        continue;
                    }
                }

                int lcl_ifail = get_inconsistent_vlas( cur_cls, flat, &cur_cls->atts[j] );

                if ( lcl_ifail != OK )
                {
                    output_scan_vla_err( cur_cls, flat, &cur_cls->atts[j], lcl_ifail, "Skipping attribute" );

                    if ( ifail == OK )
                    {
                        ifail = lcl_ifail;
                    }
                    continue;
                }

                (*attr_cnt)++;

                if ( cur_cls->atts[j].uid_cnt > 0 )
                {
                    output_scan_vla_details( VSR_DATA_LINE, cur_cls, flat, &cur_cls->atts[j] );
                    *accum_cnt += cur_cls->atts[j].uid_cnt;

                    // Output parallel VLA data. 
                    if ( !args->min_flag )
                    {
                        output_scan_vla_parallel_data( cur_cls, flat, &cur_cls->atts[j] );
                    }

                    // Free up memory used to temporary hold UIDs.
                    SM_free( cur_cls->atts[j].uids );
                    cur_cls->atts[j].uids = NULL;
                    cur_cls->atts[j].uid_cnt = 0;
                }
                else if ( args->debug_flag == TRUE || args->verbose_flag == TRUE )
                {
                    output_scan_vla_msg( cur_cls, flat, &cur_cls->atts[j], "0 inconsistent VLAs found" );
                }
            }
        }
    }
    return ifail;
}


/*------------------------------------------------------------------------
** Searches VLA attributes for VLAs that are inconsistent. 
** ----------------------------------------------------------------------- */
static int output_vlas( cls_t* cls, int* accum_cnt, int* attr_cnt, char** last_attr_name )
{
    int ifail = OK;
    int att_cnt = cls->att_cnt;

    for ( int j = 0; j < att_cnt; j++ )
    {
        if ( args->attribute_flag )
        { 
            if ( strcmp( cls->atts[j].name, args->attribute ) != 0 )
            {
                continue;
            }
        }

        int lcl_ifail = get_inconsistent_vlas( cls, NULL, &cls->atts[j] );

        if ( lcl_ifail != OK )
        {
            output_att_err( cls, NULL, &cls->atts[j], lcl_ifail, "Skipping attribute" );

            if ( ifail == OK )
            {
                ifail = lcl_ifail;
            }
            continue;
        }

        ( *last_attr_name ) = cls->atts[j].name;

        if ( cls->atts[j].uid_cnt > 0 )
        {
            output_scan_vla_details( VSR_DATA_LINE, cls, NULL, &cls->atts[j] );
            *accum_cnt += cls->atts[j].uid_cnt;

            // Output parallel VLA data. 
            if ( !args->min_flag )
            {
                output_scan_vla_parallel_data( cls, NULL, &cls->atts[j] );
            }

            // Free up memory used to temporary hold UIDs.
            SM_free( cls->atts[j].uids );
            cls->atts[j].uids = NULL;
            cls->atts[j].uid_cnt = 0;
        }
        else if ( args->debug_flag == TRUE || args->verbose_flag == TRUE )
        {
            output_scan_vla_msg( cls, NULL, &cls->atts[j], "0 inconsistent VLAs found" );
        }

        ( *attr_cnt )++;

        int remainder = ( *attr_cnt ) % 100;

        if ( remainder == 0 )
        {
            std::stringstream msg;
            msg << "Attributes processed = " << *attr_cnt << " of " << args->att_cnt << ". Inconsistent VLAs found = " << *accum_cnt;
            cons_out( msg.str() );
        }
    }

    return ifail;
}

/*-----------------------------------------------------------------*/
static std::string get_vla_class_table( const cls_t* cls, const cls_t* flat )
{
    std::string ret;

    if ( flat != NULL )
    {
        ret = flat->db_name;
    }
    else
    {
        ret = cls->db_name;
    }

    return ret;
}

/*-----------------------------------------------------------------*/
static std::string get_vla_table( const att_t* att )
{
    if ( !isVLA( att ) )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "A non-VLA attribute (%s) was passed into get_vla_table().", att->name );
    }

    return (att->db_name);
}

/*-----------------------------------------------------------------*/
static std::string get_vla_count_column( const cls_t* cls, const att_t* att )
{
    if ( !isVLA( att ) )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "A non-VLA attribute (%s) was passed into get_vla_count_column().", att->name );
    }

    std::stringstream ret;
    ret << "VLA_" << cls->cls_id << "_" << att->att_id;
    return (ret.str( ));
}

/*-----------------------------------------------------------------*/
static std::string get_vla_seq_column( const att_t* att )
{
    if ( !isVLA( att ) )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "A non-VLA attribute (%s) was passed into get_vla_seq_column().", att->name );
    }

    return ("pseq");
}

/* ************************************* */
/* VLA scanning functionality ends here. */
/* ************************************* */

/*-----------------------------------------------------------------
** Create a bitwise AND clause for the specified column and value.
*/
static std::string bitwise_and( const char* column, int value )
{
    std::stringstream ret;

    if ( EIM_dbplat() == EIM_dbplat_oracle )
    {
        if ( column != NULL && strlen( column ) > 0 )
        {
            ret << "BITAND(" << column << ", " << value << ") = " << value;
        }
    }
    else 
    {
        if ( column != NULL && strlen( column ) > 0 )
        {
            ret << "(" << column << " & " << value << ") = " << value;
        }
    }

    return ret.str( );
}

/*-----------------------------------------------------------------
** Count all backpointer references from the "from_uid" to the "to_uid".
** This counts all references from the specified class up through POM_object.
*/
static int find_ref_count( int from_cpid, const std::string from_uid, const std::string to_uid, int to_cpid, int* ref_count )
{
    int ifail = OK;
    OM_class_t to_om_class = OM_null_c;
    const cls_t *flat_p = NULL;  // pointer to a flat class.
    
    if( to_cpid > 0 )
    {
        to_om_class = DDS_class_id_of_pid( to_cpid );
    }

    /* Get class hierarchy */
    std::vector< hier_t > hier;
    getHierarchy( hier );

    /* Get system metadata of all typed and untyped references */
    std::vector< cls_t > meta;
    getRefMeta( meta, hier );

    if( args->debug_flag ) 
    {
        dumpHierarchy( hier ); 
        dumpRefMetadata( meta );
    }

    /* Loop through each class and each reference    */
    /* looking for a reference to the specified UID. */
    cls_t* class_p = NULL;

    /* Find the from_class to start with and then move up the hiearchy */
    int cls_id = from_cpid;
    hier_t* hptr = NULL;

    if(cls_id > 0)
    {
        // Determine if the specified class (object class) is flattened.
        hptr = &hier[cls_id];

        if ( hptr->cls_pos >= 0)
        {
            class_p = &meta[hptr->cls_pos];

            if( isFlat( class_p))
            {
                flat_p = class_p;
            } 
        }
    }

    while (cls_id > 0)
    {
        hptr = &hier[cls_id];

        /* Does class have attributes with references?*/
        if (hptr->refs > 0 && hptr->cls_pos >= 0)
        {
            class_p = &meta[hptr->cls_pos];
            break;
        }
        cls_id = hptr->par_id;
    }

    /* Search reference attributes of the specific class.*/
    while( class_p != NULL )
    {
        int att_cnt = class_p->att_cnt;
        int count = 0;

        for( int j = 0; j < att_cnt; j++ ) 
        {

            if( (class_p->atts[j].pproperties & OM_attr_prop_no_pom_backpointer) == OM_attr_prop_no_pom_backpointer )
            {
                continue;
            }

            // This is an optimization to avoid looking for UIDs in typed references
            // that are not associated with the class of the object we are looking for. 
            // If this is the case then skip this attribute.
            if ( class_p->atts[j].pptype == DDS_type_typed_ref && to_om_class > OM_invalid_c && to_cpid > 0 )
            {
                int typed_cpid = class_p->atts[j].ref_cpid;
                OM_class_t typed_om_class = OM_null_c;

                if( typed_cpid > 0 )
                {
                    typed_om_class = DDS_class_id_of_pid( typed_cpid );
                }

                if( typed_om_class > OM_invalid_c && typed_om_class != to_om_class )
                {
                    if( !OM_is_subclass( to_om_class, typed_om_class ) )
                    {
                        continue;
                    }
                }
            }
 
            ifail = get_ref_cnt( class_p, flat_p, &(class_p->atts[j]), from_uid, to_uid, &count );

            if( ifail != OK )
            {
                break;
            }
        }

        if( ifail != OK )
        {
            break;
        }

        *ref_count += count;

        /* Done with this class's attributes.       */
        /* Move up the hiearachy to the next class. */

        int cls_id   = class_p->cls_id;
        class_p      = NULL;
        hier_t* hptr = NULL;

        while( cls_id > 0 )
        {
            hptr   = &hier[cls_id];
            cls_id = hptr->par_id;
            hptr   = &hier[cls_id];

            // If this class has refs then stop here and count htem.
            if( hptr->refs > 0 && hptr->cls_pos >= 0 )
            {
                class_p = &meta[hptr->cls_pos];
                break;
            }
        }
    }

    return( ifail );
}

// Return the typed and untyped metadata
static int getRefMeta( std::vector< cls_t > &meta, std::vector< hier_t > &hier )
{
    return ( getMetadata( meta, hier, ref ));
}

// Append reference metadata for standard tables
static int appendTableRefMeta(std::vector< cls_t >& meta, std::vector< hier_t >& hier)
{
    int ifail = POM_ok;
    int row_cnt = 2;
    cls_t* alloc_class = (cls_t*)SM_calloc(1, sizeof(cls_t));
    att_t* alloc_atts = (att_t*)SM_calloc_persistent(row_cnt, sizeof(att_t));
    alloc_class->att_cnt = row_cnt;
    alloc_class->atts = alloc_atts;

    // Class / Table metadata
    strncpy(alloc_class->name, "POM_BACKPOINTER", CLS_NAME_SIZE);
    alloc_class->name[CLS_NAME_SIZE] = '\0';

    strncpy(alloc_class->db_name, "POM_BACKPOINTER", CLS_DB_NAME_SIZE);
    alloc_class->db_name[CLS_DB_NAME_SIZE] = '\0';

    alloc_class->cls_id = hier.size();
    alloc_class->properties = TBL_META_CLASS_PROPERTIES;

    // Attribute / Column metadata (FROM_UID / FROM_CLASS)
    strncpy(alloc_atts[0].name, "from_uid", ATT_NAME_SIZE);
    alloc_atts[0].name[ATT_NAME_SIZE] = '\0';

    strncpy(alloc_atts[0].db_name, "from_class", ATT_DB_NAME_SIZE);
    alloc_atts[0].db_name[ATT_DB_NAME_SIZE] = '\0';

    alloc_atts[0].ptype = COL_META_UT_REF_PTYPE;
    alloc_atts[0].pptype = COL_META_UT_REF_PPTYPE;
    alloc_atts[0].plength = COL_META_LENGTH;
    alloc_atts[0].att_id = 1;
    alloc_atts[0].pproperties = COL_META_PROPERTIES;
    alloc_atts[0].ref_cpid = 0;

    // Attribute / Column metadata (TO_UID / TO_CLASS)
    strncpy(alloc_atts[1].name, "to_uid", ATT_NAME_SIZE);
    alloc_atts[1].name[ATT_NAME_SIZE] = '\0';

    strncpy(alloc_atts[1].db_name, "to_class", ATT_DB_NAME_SIZE);
    alloc_atts[1].db_name[ATT_DB_NAME_SIZE] = '\0';

    alloc_atts[1].ptype = COL_META_UT_REF_PTYPE;
    alloc_atts[1].pptype = COL_META_UT_REF_PPTYPE;
    alloc_atts[1].plength = COL_META_LENGTH;
    alloc_atts[1].att_id = 2;
    alloc_atts[1].pproperties = COL_META_PROPERTIES;
    alloc_atts[1].ref_cpid = 0;

    meta.push_back(*alloc_class);

    hier[alloc_class->cls_id].refs = row_cnt;
    hier[alloc_class->cls_id].cls_pos = meta.size() - 1;

#ifdef REF_MGR_DEBUG
    hier_t h = hier[alloc_class->cls_id];
    std::stringstream msg;
    msg << "alloc_class->cls_id = " << alloc_class->cls_id << "  hier[alloc_class->cls_id].refs = " << hier[alloc_class->cls_id].refs << "  hier[alloc_class->cls_id].cls_pos = " << hier[alloc_class->cls_id].cls_pos << ".\n";
    msg << "Hierarchy class id = " << h.cls_id << "   refs = " << h.refs << "   cls_pos = " << h.cls_pos << ".\n";
    cons_out(msg.str());
#endif

    SM_free((void*)alloc_class);
    alloc_class = NULL;
    alloc_atts = NULL;

    return ifail;
}

// Return the external reference metadata
static int getExtRefMeta( std::vector< cls_t > &meta, std::vector< hier_t > &hier )
{
    return ( getMetadata( meta, hier, ext_ref ));
}

// Return the metadata for VLAs to be processed.
static int getVlaMeta( std::vector< cls_t > &meta, std::vector< hier_t > &hier )
{
    return ( getMetadata( meta, hier, vla ));
};

/*------------------------------------------------------------------------------------- **
/* Returns metadata for either typed & untyped references, external references, or VLAs.
**------------------------------------------------------------------------------------- */
static int getMetadata( std::vector< cls_t > &meta, std::vector< hier_t > &hier, Meta_Type m_type )
{
    int ifail = OK;
    logical trans_was_active=true;
    EIM_select_var_t vars[12];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }   

    std::stringstream sql;   
    sql << "SELECT a.pname as cls, a.ptname as cls_tbl, a.pcpid, a.pproperties as cls_prop, b.pname as att, b.pdbname as att_db, b.ptype, b.pptype, b.plength, b.papid, b.pproperties"; 

    if( EIM_dbplat() == EIM_dbplat_oracle )
    {
        sql << ", NVL( c.pcpid, 0 ) as ref_cpid";
    }

    if( EIM_dbplat() == EIM_dbplat_mssql )
    {
        sql << ", ISNULL( c.pcpid, 0 ) as ref_cpid";
    }

    if( EIM_dbplat() == EIM_dbplat_postgres )
    {
        sql << ", COALESCE( c.pcpid, 0 ) as ref_cpid";
    }

    sql << " FROM PPOM_CLASS a";
    sql << " INNER JOIN PPOM_ATTRIBUTE b on a.puid = b.rdefining_classu";
    sql << " LEFT  JOIN PPOM_CLASS c on b.rreferenced_classu = c.puid";

    switch ( m_type )
    {
    case ref:
        sql << " WHERE a.ptname is not NULL and b.pdbname is not NULL and (b.pptype = " << DDS_type_typed_ref << " or b.pptype = " << DDS_type_untyped_ref << ")";
        break;

    case ext_ref:
        sql << " WHERE a.ptname is not NULL and b.pdbname is not NULL and (b.pptype = " << DDS_type_external_ref << ")";
        break;

    case vla:
        sql << " WHERE a.ptname is not NULL and b.pdbname is not NULL and (b.plength = -1)";
        break;

    default:
        ERROR_raise( ERROR_line, POM_internal_error, "Invalid metadata type has been specified." );
    }

    sql << " ORDER BY pcpid, papid"; 

    EIM_select_col( &(vars[0]), EIM_varchar,  "cls",         CLS_NAME_SIZE,      false );
    EIM_select_col( &(vars[1]), EIM_varchar,  "cls_tbl",     CLS_DB_NAME_SIZE,   false );
    EIM_select_col( &(vars[2]), EIM_integer,  "pcpid",       sizeof(int),        false );
    EIM_select_col( &(vars[3]), EIM_integer,  "cls_prop",    sizeof(int),        false );
    EIM_select_col( &(vars[4]), EIM_varchar,  "att",         ATT_NAME_SIZE,      false );
    EIM_select_col( &(vars[5]), EIM_varchar,  "att_db",      ATT_DB_NAME_SIZE,   false );
    EIM_select_col( &(vars[6]), EIM_integer,  "ptype",       sizeof(int),        false );
    EIM_select_col( &(vars[7]), EIM_integer,  "pptype",      sizeof(int),        false );
    EIM_select_col( &(vars[8]), EIM_integer,  "plength",     sizeof(int),        false );
    EIM_select_col( &(vars[9]), EIM_integer,  "papid",       sizeof(int),        false );
    EIM_select_col( &(vars[10]), EIM_integer, "pproperties", sizeof(int),        false );
    EIM_select_col( &(vars[11]), EIM_integer, "ref_cpid",    sizeof(int),        false );
    EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 12, vars, 0, NULL );
    EIM_check_error( "Retrieving reference metadata\n" );

    if( report != NULL )
    {
        for (row = report; row != NULL; row = row->next) args->att_cnt++;

        for (row = report; row != NULL; )
        {
            int row_cnt = 1;
            EIM_row_p_t first = row;
            EIM_row_p_t next  = row->next;

            // Count the number of attributes for this class
            while( next != NULL )
            {
                int* first_id = NULL;
                int* next_id = NULL; 
                EIM_find_value (headers, first->line, "pcpid", EIM_integer, &first_id);
                EIM_find_value (headers, next->line,  "pcpid", EIM_integer, &next_id);

                if( *first_id == *next_id ) 
                {
                    row_cnt++;
                }
                else
                {
                    break;
                }
                next = next->next;
            }

            // Allocate class and attribute structures
            char* tmp_str = NULL;
            int*  tmp_int = NULL;

            cls_t* alloc_class     = (cls_t*)SM_calloc( 1, sizeof(cls_t) );
            att_t* alloc_atts      = (att_t*)SM_calloc_persistent( row_cnt, sizeof(att_t) );
            alloc_class->att_cnt   = row_cnt;
            alloc_class->atts      = alloc_atts;

            // Get class information
            EIM_find_value (headers, row->line, "cls", EIM_char, &tmp_str);
            strncpy( alloc_class->name, tmp_str, CLS_NAME_SIZE );
            alloc_class->name[CLS_NAME_SIZE] = '\0';

            EIM_find_value (headers, row->line, "cls_tbl", EIM_char, &tmp_str);
            strncpy( alloc_class->db_name, tmp_str, CLS_DB_NAME_SIZE );
            alloc_class->db_name[CLS_DB_NAME_SIZE] = '\0';

            EIM_find_value (headers, row->line, "pcpid", EIM_integer, &tmp_int);
            alloc_class->cls_id = *tmp_int;

            EIM_find_value (headers, row->line, "cls_prop", EIM_integer, &tmp_int);
            alloc_class->properties = *tmp_int;

            // Get attribute information

            for (int i=0; i<row_cnt && row != NULL; row = row->next, i++)
            {
                EIM_find_value (headers, row->line, "att", EIM_char, &tmp_str);
                strncpy( alloc_atts[i].name, tmp_str, ATT_NAME_SIZE );
                alloc_atts[i].name[ATT_NAME_SIZE] = '\0';

                EIM_find_value (headers, row->line, "att_db", EIM_char, &tmp_str);
                strncpy( alloc_atts[i].db_name, tmp_str, ATT_DB_NAME_SIZE );
                alloc_atts[i].db_name[ATT_DB_NAME_SIZE] = '\0';

                EIM_find_value (headers, row->line, "ptype", EIM_integer, &tmp_int);
                alloc_atts[i].ptype = *tmp_int;

                EIM_find_value (headers, row->line, "pptype", EIM_integer, &tmp_int);
                alloc_atts[i].pptype = *tmp_int;

                EIM_find_value (headers, row->line, "plength", EIM_integer, &tmp_int);
                alloc_atts[i].plength = *tmp_int;

                EIM_find_value (headers, row->line, "papid", EIM_integer, &tmp_int);
                alloc_atts[i].att_id = *tmp_int;

                EIM_find_value (headers, row->line, "pproperties", EIM_integer, &tmp_int);
                alloc_atts[i].pproperties = *tmp_int;

                EIM_find_value (headers, row->line, "ref_cpid", EIM_integer, &tmp_int);
                alloc_atts[i].ref_cpid = *tmp_int;
            }

            meta.push_back( *alloc_class );

            if( hier.size() > 0 )
            {
                hier[alloc_class->cls_id].refs = row_cnt;
                hier[alloc_class->cls_id].cls_pos = meta.size()-1;

                hier_t *h_ptr  = &(hier[alloc_class->cls_id]);
                h_ptr->refs    = row_cnt;
                h_ptr->cls_pos = meta.size()-1;

#ifdef REF_MGR_DEBUG
                hier_t h  = hier[alloc_class->cls_id];
                std::stringstream msg;
                msg << "alloc_class->cls_id = " << alloc_class->cls_id << "  hier[alloc_class->cls_id].refs = " << hier[alloc_class->cls_id].refs << "  hier[alloc_class->cls_id].cls_pos = " << hier[alloc_class->cls_id].cls_pos << ".\n";  
                msg << "Hierarchy class id = " << h.cls_id << "   refs = " << h.refs << "   cls_pos = " << h.cls_pos << ".\n";
                cons_out( msg.str() );
#endif
            }

            SM_free( (void *)alloc_class );

            if( row == NULL )
            {
                break;
            }  
        }
    }

    EIM_free_result( headers, report );
    headers = NULL;
    report = NULL;

    // Ensure that we have retrieved all the flattened classes. 
    std::stringstream sql2;
    sql2 << "SELECT a.pname as cls, a.ptname as cls_tbl, a.pcpid, a.pproperties as cls_prop ";
    sql2 << " FROM PPOM_CLASS a  WHERE " << bitwise_and( "a.pproperties", POM_class_prop_has_flat_tables );
    EIM_exec_sql_bind( sql2.str( ).c_str( ), &headers, &report, 0, 4, vars, 0, NULL );
    EIM_check_error( "Retrieving flattened class metadata\n" );

    if ( report != NULL )
    {
        for ( row = report; row != NULL; row = row->next )
        {
            // Allocate class structure
            char* tmp_str = NULL;
            int* tmp_int = NULL;

            cls_t* alloc_class = (cls_t*)SM_calloc( 1, sizeof( cls_t ) );
            alloc_class->att_cnt = 0;
            alloc_class->atts = NULL;

            // Get class information
            EIM_find_value( headers, row->line, "cls", EIM_char, &tmp_str );
            strncpy( alloc_class->name, tmp_str, CLS_NAME_SIZE );
            alloc_class->name[CLS_NAME_SIZE] = '\0';

            EIM_find_value( headers, row->line, "cls_tbl", EIM_char, &tmp_str );
            strncpy( alloc_class->db_name, tmp_str, CLS_DB_NAME_SIZE );
            alloc_class->db_name[CLS_DB_NAME_SIZE] = '\0';

            EIM_find_value( headers, row->line, "pcpid", EIM_integer, &tmp_int );
            alloc_class->cls_id = *tmp_int;

            EIM_find_value( headers, row->line, "cls_prop", EIM_integer, &tmp_int );
            alloc_class->properties = *tmp_int;

            // Ensure that we are working with a flattened class.
            if ( (alloc_class->properties & POM_class_prop_has_flat_tables) != POM_class_prop_has_flat_tables )
            {
                ERROR_raise( ERROR_line, POM_internal_error, "getMetadata(): Queried flattened classes, but retrieved non-flatten class %d", alloc_class->cls_id );
                // SM_free( alloc_class );
                // continue;
            }

            // Ensure that the class metadata has not already been read into the metadata cache.
            if ( hier[alloc_class->cls_id].cls_pos != -1 )
            {
                if ( (hier[alloc_class->cls_id].flags & IS_FLAT_CLASS) != IS_FLAT_CLASS )
                {
                    ERROR_raise( ERROR_line, POM_internal_error, "getMetadata(): Hiearchy class is NOT a flattened class %d", alloc_class->cls_id );
                }
                // Validate that class is indeed a flattened classe. 
                SM_free( alloc_class );
                continue;
            }

            // Add new class entry to the metadata cache and update the hierarchy  
            meta.push_back( *alloc_class );
            hier[alloc_class->cls_id].refs = 0;
            hier[alloc_class->cls_id].cls_pos = meta.size( ) - 1;

            SM_free( (void*)alloc_class );

            if ( row == NULL )
            {
                break;
            }
        }
    }

    EIM_free_result( headers, report );
    headers = NULL;
    report = NULL;

    if( !trans_was_active )
    {
        EIM_commit_transaction( "getMetadata()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details");
    cons_out( msg );
    if(!trans_was_active)
    {
            EIM__clear_transaction( ERROR_ask_failure_code() );
            ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
            ERROR_reraise();
    }
    ERROR_END


    return( ifail );
}



/*-----------------------------------------------------------------*/
static int dumpRefMetadata( const std::vector< cls_t > &meta )
{
    int ifail = OK;

    cons_out( "\nLOADED CLASS AND ATTRIBUTE METADATA:" );
    const std::string hdr("Class,Attribute,class_table,att_db,cls_properties,pcpid,ptype,pptype,plength,papid,pproperties,ref_cpid,");
    cons_out( hdr );

    for( int i=0; i<meta.size(); i++ )
    {
        if ( meta[i].att_cnt > 0 )
        {
            for( int j=0; j<meta[i].att_cnt; j++) 
            {
                std::stringstream out;
                out << meta[i].name    << "," << meta[i].atts[j].name     << ",";
                out << meta[i].db_name << "," << meta[i].atts[j].db_name  << ",";
                out << meta[i].properties                                 << ",";
                out << meta[i].cls_id                                     << ",";
                out << meta[i].atts[j].ptype                              << ",";
                out << meta[i].atts[j].pptype                             << ",";
                out << meta[i].atts[j].plength                            << ",";
                out << meta[i].atts[j].att_id                             << ",";
                out << meta[i].atts[j].pproperties                        << ",";
                out << meta[i].atts[j].ref_cpid                           << ",";

                cons_out( out.str() );
            }
        }
        else
        {
            std::stringstream out;
            out << meta[i].name << ",,";
            out << meta[i].db_name << ",,";
            out << meta[i].properties << ",";
            out << meta[i].cls_id << ",";
            out << ",";
            out << ",";
            out << ",";
            out << ",";
            out << ",";
            out << ",";

            cons_out( out.str( ) );
            }
        }
    const std::string cr("\n");
    cons_out( cr );

    return( ifail );
}


/*-----------------------------------------------------------------*/
static int getHierarchy( std::vector< hier_t > &hier )
{
    int ifail = OK;
    logical trans_was_active=true;
    EIM_select_var_t vars[3];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }   

    std::stringstream sql;   

    if( EIM_dbplat() == EIM_dbplat_oracle )
    {
        sql << "SELECT a.pcpid as pcpid, NVL( c.pcpid, 0 ) as par_cpid";
    }

    if( EIM_dbplat() == EIM_dbplat_mssql )
    {
        sql << "SELECT a.pcpid as pcpid, ISNULL( c.pcpid, 0 ) as par_cpid";
    }

    if( EIM_dbplat() == EIM_dbplat_postgres )
    {
        sql << "SELECT a.pcpid as pcpid, COALESCE( c.pcpid, 0 ) as par_cpid";
    }

    sql << ", a.pproperties FROM PPOM_CLASS a";
    sql << " LEFT  JOIN PPOM_CLASS c on a.psuperclass = c.pname"; 
    sql << " ORDER BY pcpid, par_cpid"; 

    EIM_select_col( &(vars[0]), EIM_integer, "pcpid",       sizeof(int),        false );
    EIM_select_col( &(vars[1]), EIM_integer, "par_cpid",    sizeof(int),        false );
    EIM_select_col( &(vars[2]), EIM_integer, "pproperties", sizeof( int ),      false );
    EIM_exec_sql_bind( sql.str( ).c_str( ), &headers, &report, 0, 3, vars, 0, NULL );
    EIM_check_error( "Retrieving hierarchy\n" );

    if( report != NULL )
    {
         int last_cls_id = 0;

         hier_t* alloc_hier0    = (hier_t*)SM_calloc( 1, sizeof(hier_t) );
         alloc_hier0->cls_pos = -1;
         hier.push_back( *alloc_hier0 );
         SM_free( (void *)alloc_hier0 ); 

        for (row = report; row != NULL; row = row->next) 
        {
            int*  tmp_int = NULL;

            hier_t* alloc_hier    = (hier_t*)SM_calloc( 1, sizeof(hier_t) );
            alloc_hier->cls_pos = -1;

            EIM_find_value (headers, row->line, "pcpid", EIM_integer, &tmp_int);

            if( *tmp_int <= last_cls_id )
            {
                ERROR_raise( ERROR_line, POM_invalid_class_id, "POM data dictionary contains invalid metadata, likely duplicate class IDs associated with a class.\n");
            }

            while( *tmp_int > last_cls_id+1 )
            {
                 last_cls_id++;
                 hier_t* alloc_hier1    = (hier_t*)SM_calloc( 1, sizeof(hier_t) );
                 alloc_hier1->cls_id    =  last_cls_id;
                 alloc_hier1->cls_pos   = -1;
                 alloc_hier1->flags     = 0;
                 hier.push_back( *alloc_hier1 );
                 SM_free( (void *)alloc_hier1 );
            }
                
            alloc_hier->cls_id = *tmp_int;

            EIM_find_value (headers, row->line, "par_cpid", EIM_integer, &tmp_int);
            alloc_hier->par_id = *tmp_int;
            
            // Check for flattened classes. 
            tmp_int = NULL;
            alloc_hier->flags = 0;
            EIM_find_value( headers, row->line, "pproperties", EIM_integer, &tmp_int );

            if ( tmp_int != NULL && (*tmp_int & POM_class_prop_has_flat_tables) == POM_class_prop_has_flat_tables )
            {
                alloc_hier->flags = IS_FLAT_CLASS;

                for ( int par = alloc_hier->par_id; par >= 0; )
                {
                    hier[par].flags = hier[par].flags | HAS_FLAT_SUBCLASSES;

                    if ( par <= 0 )
                    {
                        break;
                    }

                    par = hier[par].par_id;
                }
            }

            hier.push_back( *alloc_hier );
            SM_free( (void *)alloc_hier ); 
            last_cls_id++;
        }
    }

    EIM_free_result( headers, report );

    if( !trans_was_active )
    {
        EIM_commit_transaction( "getHierarchy()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details");
    cons_out( msg );
    if(!trans_was_active)
    {
            EIM__clear_transaction( ERROR_ask_failure_code() );
            ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
            ERROR_reraise();
    }
    ERROR_END


    return( ifail );
}


/*-----------------------------------------------------------------*/
static int dumpHierarchy( std::vector< hier_t > &hier )
{
    int ifail = OK;

    cons_out( "\nCLASS HIERARCHY:" );
    const std::string hdr( "ClassID,ParentID,Refs,Flags," );
    cons_out( hdr );

    for( int i=0; i<hier.size(); i++ )
    {
        std::stringstream out;
        out << hier[i].cls_id << "," << hier[i].par_id << "," << hier[i].refs << "," << hier[i].flags << ",";
        cons_out( out.str() );
    }

    const std::string cr("\n");
    cons_out( cr );

    return( ifail );
}

/*-----------------------------------------------------------------*/
static int validate_cmd_line_class_and_attribute_params( )
{
    int ifail = OK;

    if ( args->class_flag || args->attribute_flag )
    {
        EIM_select_var_t vars[2];
        EIM_bind_var_t bind_vars[2];
        int var_cnt = 0;
        EIM_value_p_t headers = NULL;
        EIM_row_p_t report = NULL;

        std::stringstream sql;

        if ( args->class_flag && args->attribute_flag )
        {
            sql << "SELECT a.pname AS class_name, b.pname AS attribute_name FROM PPOM_CLASS a, PPOM_ATTRIBUTE b WHERE a.puid = b.rdefining_classu AND a.pname = :1 AND b.pname = :2";
            EIM_select_col( &(vars[0]), EIM_varchar, "class_name", CLS_NAME_SIZE + 1, false );
            EIM_select_col( &(vars[1]), EIM_varchar, "attribute_name", ATT_NAME_SIZE + 1, false );
            EIM_bind_val  ( &bind_vars[0], EIM_varchar, strlen( args->class_n ) + 1, args->class_n );
            EIM_bind_val  ( &bind_vars[1], EIM_varchar, strlen( args->attribute ) + 1, args->attribute );
            var_cnt = 2;
        }
        else if ( args->class_flag )
        {
            sql << "SELECT a.pname AS class_name FROM PPOM_CLASS a WHERE a.pname = :1";
            EIM_select_col( &(vars[0]), EIM_varchar, "class_name", CLS_NAME_SIZE + 1, false );
            EIM_bind_val  ( &bind_vars[0], EIM_varchar, strlen( args->class_n ) + 1, args->class_n );
            var_cnt = 1;
        }
        else if ( args->attribute_flag )
        {

            sql << "SELECT b.pname AS attribute_name FROM PPOM_ATTRIBUTE b WHERE b.pname = :1";
            EIM_select_col( &(vars[0]), EIM_varchar, "attribute_name", ATT_NAME_SIZE + 1, false );
            EIM_bind_val  ( &bind_vars[0], EIM_varchar, strlen( args->attribute ) + 1, args->attribute );
            var_cnt = 1;
        }


        // Execute query class name or attribute name.
        ERROR_PROTECT

        EIM_exec_sql_bind( sql.str( ).c_str( ), &headers, &report, 0, var_cnt, vars, var_cnt, bind_vars );
        EIM_check_error( "validate_cmd_line_class_and_attribute_params()\n" );

        if ( report == NULL )
        {
            ifail = POM_invalid_string;
            std::stringstream msg;

            if ( args->class_flag && args->attribute_flag )
            {
                msg << "\nError " << ifail << " Either the class name (" << args->class_n << ") or the attribute name (" << args->attribute << ") is invalid.";
            }
            else if ( args->class_flag )
            {
                msg << "\nError " << ifail << " Class name (" << args->class_n << ") is invalid.";
            }
            else if ( args->attribute_flag )
            {
                msg << "\nError " << ifail << " Attribute name (" << args->attribute << ") is invalid.";
            }
            cons_out( msg.str( ) );
        }

        ERROR_RECOVER
        const std::string msg( "EXCEPTION while validating class or attribute names. See syslog for additional details." );
        cons_out( msg );
        ERROR_reraise( );
        ERROR_END

        EIM_free_result( headers, report );
        headers = NULL;
        report = NULL;
    }

    return ifail;
}

/*-----------------------------------------------------------------*/
static logical isScalar( const att_t *att )
{
    logical ret = false;

    if( att != NULL && att->plength == 1 )
    {
        ret = true;
    }

    return( ret );
}

/*-----------------------------------------------------------------*/
static logical isVLA( const att_t *att )
{
    logical ret = false;

    if( att != NULL && att->plength == -1 )
    {
        ret = true;
    }

    return( ret );
}

/*-----------------------------------------------------------------*/
static logical isLA( const att_t *att )
{
    logical ret = false;

    if( att != NULL && att->plength >= DDS_array_value_g )
    {
        ret = true;
    }

    return( ret );
}

/*-----------------------------------------------------------------*/
static logical isSA( const att_t *att )
{
    logical ret = false;

    if( att != NULL && att->plength > 1 && att->plength < DDS_array_value_g )
    {
        ret = true;
    }

    return( ret );
}

/*-----------------------------------------------------------------*/
static logical isFlat( const cls_t *cls )
{
    logical ret = false;

    if( cls != NULL && (cls->properties & OM_class_prop_has_flat_tables) != 0 )
    {
        ret = true;
    }

    return( ret );
}

/*-----------------------------------------------------------------
** Is this metadata for a column on a table that is outside of the 
** POM class hiearchy?
**-----------------------------------------------------------------*/
static logical isColumn(const att_t* att)
{
    logical ret = false;

    if (att != NULL && att->plength == COL_META_LENGTH )
    {
        ret = true;
    }

    return(ret);
}

/*-----------------------------------------------------------------*/
static void output_att_err( const cls_t *cls, const cls_t *flat, const att_t *att, int ifail, const char* msg )
{
    std::stringstream out_msg;

    out_msg << " ERROR " << cls->name << ":" << att->name << " (" << get_ref_table_and_column( cls, flat, att, -1) << "): ifail = " << ifail;

    if( msg != NULL )
    {
        out_msg << " - " << msg;
    }

    cons_out( out_msg.str() );
}

/*-----------------------------------------------------------------*/
static void output_att_msg( const cls_t *cls, const cls_t *flat, const att_t *att, const char* msg )
{
    std::stringstream out_msg;

    if( flat == NULL )
    {
        out_msg << "       " << cls->name << ":" << att->name << " (" << get_ref_table_and_column( cls, flat, att, -1) << "): " << msg;
    }
    else
    {
        out_msg << " FLAT  " << cls->name << ":" << att->name << " (" << get_ref_table_and_column( cls, flat, att, -1) << "): " << msg;
    }
    cons_out( out_msg.str() );
}

/*-----------------------------------------------------------------*/
static void output_att_data( const char prefix, const cls_t *cls, const cls_t *flat, const att_t *att )
{
    std::stringstream out_msg;

    if ( args->min_flag )
    {
        out_msg << "\n ";
    }
    else
    {
       if ( prefix == VSR_DATA_LINE )
        {
            out_msg << "\n" << VSR_HDR_LINE;
        }
        else
        {
            out_msg << "\n" << prefix;
        }
    }

    std::string storage_mode = get_storage_mode( flat != NULL ? flat->name : cls->name );

    if( flat == NULL )
    {
        out_msg << "  " << storage_mode << " " << cls->name << ":" << att->name << "[" << att->pptype << "] (" << get_ref_table_and_column( cls, flat, att, -1) <<  "): " << att->uid_cnt << " references found";
    }
    else
    {
        out_msg << "  " << storage_mode << " " << flat->name << ":" << flat->name << "\\" << cls->name << ":" << att->name << "[" << att->pptype << "] (";
        out_msg << get_ref_table_and_column( cls, flat, att, -1) <<  "): " << att->uid_cnt << " references found";
    }

    if ( args->min_flag )
    {
        for ( int i = 0; i < att->uid_cnt; i++ )
        {
            out_msg << "\n        " << att->uids[i].uid;
        }
    }
    else
    {
        for ( int i = 0; i < att->uid_cnt; i++ )
        {
            out_msg << "\n.       " << att->uids[i].uid;
        }
    }


    cons_out( out_msg.str() );
}


/*-----------------------------------------------------------------*/
static std::string get_ref_table( const cls_t *cls, const cls_t *flat, const att_t *att )
{
    if( isScalar( att ) || isSA( att ) || isColumn( att ))
    {
        if( flat != NULL )
        {
            std::string ret( flat->db_name );
            return( ret );
        }
        else
        {
            std::string ret( cls->db_name );
            return( ret );
        } 
    }

    std::string ret( att->db_name );
    return( ret );
}


/*-----------------------------------------------------------------*/
static std::string get_ref_column( const att_t *att, int sa_offset, bool for_dsp )
{
    std::stringstream ret;

    if( isScalar( att ) )
    {
        if( att->pptype == DDS_type_external_ref )
        {
            ret << att->db_name;
        }
        else if( att->pptype == DDS_type_typed_ref || att->pptype == DDS_type_untyped_ref )
        {
            ret << att->db_name << DDS_u_suffix;
        }
        else 
        {
            ret << att->db_name;
        }
        return( ret.str() );
    }

    if( isVLA( att ) )
    {
        std::string ret;

        if( att->pptype == DDS_type_external_ref )
        {
            ret = "pval_0";
        }
        else if( att->pptype == DDS_type_typed_ref || att->pptype == DDS_type_untyped_ref )
        {
            ret = "pvalu_0";
        }
        else 
        {
            ret = "pval_0";
        }
        return( ret );
    }

    if( isLA( att ) )
    {
        std::string ret;

        if( att->pptype == DDS_type_external_ref )
        {
            ret = "pval";
        }
        else if( att->pptype == DDS_type_typed_ref || att->pptype == DDS_type_untyped_ref )
        {
            ret = "pvalu";
        }
        else 
        {
            ret = "pval";
        }
        return( ret );
    }

    if ( isColumn( att ) )
    {
        return att->name;
    }

    ret << att->db_name;
    
    if( sa_offset >= 0 )
    {
        if ( !for_dsp )
        {
            // To be used in SQL
            if ( att->pptype == DDS_type_external_ref )
            {
                ret << "_" << sa_offset;
            }
            else if ( att->pptype == DDS_type_typed_ref || att->pptype == DDS_type_untyped_ref )
            {
                ret << "_" << sa_offset << DDS_u_suffix;
            }
            else
            {
                ret << "_" << sa_offset;
            }
        }
        else
        {
            // To be output to console
            if ( att->pptype == DDS_type_external_ref )
            {
                ret << "_[0-" << (att->plength -1) << "]";
            }
            else if ( att->pptype == DDS_type_typed_ref || att->pptype == DDS_type_untyped_ref )
            {
                ret << "_[0-" << ( att->plength - 1 ) << "]" << DDS_u_suffix;
            }
            else
            {
                ret << "_[0-" << ( att->plength - 1 ) << "]";
            }        
        }
    }

    return( ret.str() );
}

/*-----------------------------------------------------------------*/
static std::string get_ref_cid_column( const att_t* att, int sa_offset, bool for_dsp = false )
{
    std::stringstream ret;

    if ( att->pptype != DDS_type_typed_ref && att->pptype != DDS_type_untyped_ref )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "get_ref_cid_column(): External references do not have class ID columns (1)" );
    }

    if ( isScalar( att ) )
    {
        ret << att->db_name << DDS_c_suffix;
        return(ret.str( ));
    }

    if ( isVLA( att ) )
    {
        return "pvalc_0";
    }

    if ( isLA( att ) )
    {
        return "pvalc";
    }

    if ( isColumn( att ) )
    {
        return att->db_name;
    }

    ret << att->db_name;

    if ( !for_dsp )
    {
        // To be used with SQL
        if ( sa_offset >= 0 )
        {
            ret << "_" << sa_offset << DDS_c_suffix;
        }
    }
    else
    {
        // To be output to console
        ret << "_[0-" << ( att->plength - 1 ) << "]" << DDS_c_suffix;   
    }

    return(ret.str( ));
}

/*-----------------------------------------------------------------*/
static std::string get_ref_col_where_expr( const att_t *att, int sa_offset, const std::string value )
{
    std::stringstream ret;

    if( !args->ext_ref_flag || value.length() > 27)
    {
        // Process typed and untyped references 
        ret << get_ref_column( att, sa_offset );
    }
    else
    {
        // Process external references
        if ( EIM_dbplat() == EIM_dbplat_oracle || EIM_dbplat() == EIM_dbplat_postgres )
        {
            ret << "SUBSTR(";     
        }
        else if ( EIM_dbplat() == EIM_dbplat_mssql )
        {
             ret << "SUBSTRING("; 
        }
        else
        {
            ERROR_raise( ERROR_line, POM_internal_error, "Invalid database platform has been specified." );
        }

        ret << get_ref_column( att, sa_offset );
        ret << ", 1, " << value.length() << ")";
    }

    ret << " = '" << value << "'";

    return( ret.str() );
}


/*-----------------------------------------------------------------*/
static std::string get_ref_table_and_column( const cls_t *cls, const cls_t *flat, const att_t *att, int sa_offset, bool for_dsp )
{
    std::stringstream ret;
    ret << get_ref_table( cls, flat, att ) << "." << get_ref_column( att, sa_offset, for_dsp );
    return( ret.str() );
}

/*-----------------------------------------------------------------*/
static std::string get_sa_where_clause( const att_t *att )
{
    std::stringstream clause;
    int seq = att->plength;

    for( int i=0; i<seq; i++ )
    {
        if( i > 0 ) 
        {
            clause << " OR ";
        }
 //     clause << get_ref_column( att, i ) << " = '" << args->uid << "'";
        clause << get_ref_col_where_expr( att, i, args->uid );
    }

    return( clause.str() );
}

/*-----------------------------------------------------------------*/
static std::string get_sa_sql_extension( const att_t *att, const std::string base_sql, const std::string to_uid )
{
    std::stringstream extension;
    int seq = att->plength;

    for( int i=0; i<seq; i++ )
    {
        if( i > 0 ) 
        {
            extension << " UNION ALL " << base_sql;
        }
//      extension << get_ref_column( att, i ) << " = '" << to_uid << "'";
        extension << get_ref_col_where_expr( att, i, to_uid );
    }

    return( extension.str() );
}


/*-----------------------------------------------------------------*/
static int get_refs( cls_t *cls, const cls_t *flat, att_t *att )
{
    int ifail = OK;
    logical trans_was_active=true;
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    int row_cnt = 0;
    ref_t *alloc_uids = NULL;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "select ";

    if( !args->noparallel_flag && EIM_dbplat() == EIM_dbplat_oracle )
    {
        sql << "/*+ parallel */ ";
    }

    if( isVLA( att ) || isLA( att ) )
    {
        sql << "distinct ";
    }

    if( EIM_dbplat() == EIM_dbplat_mssql )
    {
        sql << "TOP " << ((args->max_ref_cnt) + 1) << " ";
    }

    sql << "puid from " << get_ref_table( cls, flat, att ) << " where ";
    
    if( isSA( att ) )
    {
        sql << "(" << get_sa_where_clause( att ) << ")";
    }
    else
    {
        sql << get_ref_col_where_expr( att, -1, args->uid );
    }

    if( EIM_dbplat() == EIM_dbplat_oracle )
    {
        sql << " AND rownum <= " << ((args->max_ref_cnt) + 1);
    }
    if( EIM_dbplat() == EIM_dbplat_postgres )
    {
        sql << " FETCH FIRST " << ((args->max_ref_cnt) + 1) << " ROWS ONLY";
    }

    EIM_select_col( &(vars[0]), EIM_varchar, "puid",         MAX_UID_SIZE,       false );
    ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if( !args->ignore_errors_flag )
    {
        EIM_check_error( "get_refs()\n" );
    }
    else if( ifail != OK )
    {
        EIM_clear_error();

        std::stringstream msg;
        msg << "\nError " << ifail << " while reading " << get_ref_table_and_column( cls, NULL, att, -1);
        msg << ". SKIPPING class:attribute " << cls->name << ":" << att->name << ".\n";
        cons_out( msg.str() );

        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    if( report != NULL )
    {
        row_cnt = 0;

        for( row = report; row != NULL; row = row->next ) row_cnt++;

        alloc_uids = (ref_t *)SM_calloc_persistent( row_cnt, sizeof(ref_t) );

        int i = 0;

        for( row=report; row != NULL; row=row->next )
        {
            char* tmp = NULL;
            EIM_find_value( headers, row->line, "puid", EIM_varchar, &tmp );
            strncpy( alloc_uids[i].uid, tmp, MAX_UID_SIZE );
            alloc_uids[i].uid[MAX_UID_SIZE] = '\0';
            i++;
        }

        att->uids = alloc_uids;
        att->uid_cnt = row_cnt;

        if( flat == NULL )
        {
            cls->ref_cnt += row_cnt;
        }
        else
        {
            cls->flt_cnt += row_cnt;
        }
        EIM_free_result( headers, report );
    }

    if( !trans_was_active )
    {
        EIM_commit_transaction( "get_refs()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details. (See -i option to ignore this error.)");
    cons_out( msg );

    if( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return( ifail );
}

/*------------------------------------------------------------------------
** Searches (non-flattened) reference attributes for objects (UIDs) that  
** reference the object (UID) specified on the command line (-uid=).
** Once references are found the information is dumped to the console. 
** ----------------------------------------------------------------------- */
static int output_refs( cls_t *cls, int *accum_cnt, int *attr_cnt, char **last_attr_name )
{
    int ifail = OK;

    if( *accum_cnt <= args->max_ref_cnt )
    {

        int att_cnt = cls->att_cnt;

        for( int j = 0; j < att_cnt && *accum_cnt <= args->max_ref_cnt; j++) 
        {
            int lcl_ifail = get_refs( cls, NULL, &cls->atts[j] );

            if( lcl_ifail != OK )
            {
                output_att_err( cls, NULL, &cls->atts[j], lcl_ifail, "Skipping attribute" );

                if( ifail == OK )
                {
                    ifail = lcl_ifail;
                }
                continue;
            }

            (*last_attr_name) = cls->atts[j].name;
                
            if( cls->atts[j].uid_cnt > 0 )
            {
                output_att_data( VSR_DATA_LINE, cls, NULL, &cls->atts[j] );
                *accum_cnt += cls->atts[j].uid_cnt;
                // Free up memory used to temporary hold UIDs. 
                SM_free( cls->atts[j].uids );
                cls->atts[j].uids = NULL;
                cls->atts[j].uid_cnt = 0;
            }
            else if( args->debug_flag == TRUE || args->verbose_flag == TRUE )
            {
                output_att_msg( cls, NULL, &cls->atts[j], "0 references found" );
            }

            (*attr_cnt)++;

            int remainder = (*attr_cnt) % 100;

            if( remainder == 0 )
            {
                std::stringstream msg;
                msg << "Attributes processed = " << *attr_cnt << " of " << args->att_cnt << ". References found = " << *accum_cnt;
                cons_out( msg.str() );
            }
        }
    }

    return ifail;
}


/*------------------------------------------------------------------------
** Searches flattened attributes for objects (UIDs) that reference 
** the object (UID) specified on the command line (-uid=).
** Once references are found the information is dumped to the console. 
** ----------------------------------------------------------------------- */
static int output_flattened_refs( const std::vector< hier_t > &hier, std::vector< cls_t > &meta, cls_t *flat, int *accum_cnt, int *attr_cnt )
{
    int ifail = OK;

    if( isFlat( flat ) ) 
    {
       if( *accum_cnt <= args->max_ref_cnt )
       {
            // Process the attributes that have been flattened into this class
            cls_t *cur_cls  = NULL;
            int cur_cpid = flat->cls_id;
            int par_cpid = hier[cur_cpid].par_id;

            // Walk up the hiearchy until we come to POM_object (cpid = 1)

            while( *accum_cnt <= args->max_ref_cnt && par_cpid > 0 && hier[par_cpid].cls_pos >= 0 )
            {
                int pos  = hier[par_cpid].cls_pos;
                cur_cls  = &meta[pos];
                cur_cpid = par_cpid;
                par_cpid = hier[cur_cpid].par_id;

                int att_cnt = cur_cls->att_cnt;

                for( int j = 0; j < att_cnt && *accum_cnt <= args->max_ref_cnt; j++) 
                {
                    // Process only attributes found on the class table.
                    if( !isScalar( &cur_cls->atts[j] ) && !isSA( &cur_cls->atts[j] ) )
                    {
                        continue;
                    }

                    int lcl_ifail = get_refs( cur_cls, flat, &cur_cls->atts[j] );

                    if( lcl_ifail != OK )
                    {
                        output_att_err( cur_cls, flat, &cur_cls->atts[j], lcl_ifail, "Skipping attribute" );

                        if( ifail == OK )
                        {
                            ifail = lcl_ifail;
                        }
                        continue;
                    }

                    ( *attr_cnt )++;
                
                    if( cur_cls->atts[j].uid_cnt > 0 )
                    {
                        output_att_data( VSR_DATA_LINE, cur_cls, flat, &cur_cls->atts[j] );
                        *accum_cnt += cur_cls->atts[j].uid_cnt;
                        // Free up memory used to temporary hold UIDs. 
                        SM_free( cur_cls->atts[j].uids );
                        cur_cls->atts[j].uids = NULL;
                        cur_cls->atts[j].uid_cnt = 0;
                    }
                    else if( args->debug_flag == TRUE || args->verbose_flag == TRUE )
                    {
                        output_att_msg( cur_cls, flat, &cur_cls->atts[j], "0 references found" );
                    }
                }
            }
        }
    }

    return ifail;
}

/*------------------------------------------------------------------------
** Searches (counts) the attribute (table and column) for the specified 
** "to UID. Only the rows with the "from" UID in the PUID column are  
** considered. If a flat class is passed in then the table from the flat  
** class is used for scalar and small array attributes. Large arrays and 
** VLAs are always associated with the defining class (cls).
** ----------------------------------------------------------------------- */
static int get_ref_cnt( const cls_t *cls, const cls_t *flat, const att_t *att, const std::string from_uid, const std::string to_uid, int* count )
{
    int ifail = OK;
    logical trans_was_active=true;
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "select ";

    if( !args->noparallel_flag && EIM_dbplat() == EIM_dbplat_oracle )
    {
        sql << "/*+ parallel */ ";
    }

    sql << "count(*) cnt from " << get_ref_table( cls, flat, att ) << " where puid = '" << from_uid << "' AND ";
    
    if( isSA( att ) )
    {
        // generate a UNION ALL for each subsequent column of the small array.
        sql << get_sa_sql_extension( att, sql.str(), to_uid );
    }
    else
    {
        sql << get_ref_col_where_expr( att, -1, to_uid );
    }

    EIM_select_col( &(vars[0]), EIM_integer, "cnt",         sizeof(int),       false );
    ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if( ifail != OK )
    {
        EIM_clear_error();

        std::stringstream msg;
        msg << " while reading " << get_ref_table_and_column( cls, flat, att, -1);
        msg << "   class:attribute = " << cls->name << ":" << att->name << "\n";
        error_out( ERROR_line, ifail, msg.str() );

        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    if( report != NULL )
    {
        for( row=report; row != NULL; row=row->next )
        {
            int* tmp = NULL;
            EIM_find_value( headers, row->line, "cnt", EIM_integer, &tmp );
            *count += *tmp;
        }
        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    if( !trans_was_active )
    {
        EIM_commit_transaction( "get_ref_cnt()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out( msg );

    if( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return( ifail );
}

/*-----------------------------------------------------------------*/
static int get_backpointers( const std::string from_uid, const std::string to_uid, std::vector<bp_t> &bptrs )
{
    int ifail = OK;

    logical trans_was_active=true;
    EIM_select_var_t vars[7];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row = NULL;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "SELECT T1.from_uid, T1.from_class, T2.pname from_class_name, T1.to_uid, T1.to_class, T3.pname to_class_name, bp_count from POM_BACKPOINTER T1 ";
    sql << "LEFT OUTER JOIN PPOM_CLASS T2 on T1.from_class = T2.pcpid ";
    sql << "LEFT OUTER JOIN PPOM_CLASS T3 on T1.to_class = T3.pcpid ";
    sql << "WHERE T1.from_uid = '" << from_uid << "' and T1.to_uid = '" << to_uid << "'"; 
    EIM_select_col( &(vars[0]),  EIM_varchar, "from_uid",         MAX_UID_SIZE+1,   false );
    EIM_select_col( &(vars[1]),  EIM_integer, "from_class",       sizeof(int),      false );
    EIM_select_col( &(vars[2]),  EIM_varchar, "from_class_name",  CLS_NAME_SIZE+1,  false );
    EIM_select_col( &(vars[3]),  EIM_varchar, "to_uid",           MAX_UID_SIZE+1,   false );
    EIM_select_col( &(vars[4]),  EIM_integer, "to_class",         sizeof(int),      false );
    EIM_select_col( &(vars[5]),  EIM_varchar, "to_class_name",    CLS_NAME_SIZE+1,  false );
    EIM_select_col( &(vars[6]),  EIM_integer, "bp_count",         sizeof(int),      false );
    int ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 7, vars, 0, NULL );

    if( ifail == OK  && report != NULL )
    {
        for( row = report; row != NULL; row = row->next )
        {
            bp_t* bptr = (bp_t*)SM_calloc( 1, sizeof(bp_t) );
            char* tmp_c;
            int*  tmp_i;

            tmp_c = NULL;
            EIM_find_value( headers, row->line, "from_uid", EIM_varchar, &tmp_c );
            if( tmp_c != NULL )
            {
                strncpy( bptr->from_uid, tmp_c, MAX_UID_SIZE );
            }

            tmp_i = NULL;
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &tmp_i );
            if( tmp_i != NULL )
            {
                bptr->from_class = *tmp_i;
            }

            tmp_c = NULL;
            EIM_find_value( headers, row->line, "from_class_name", EIM_varchar, &tmp_c );
            if( tmp_c != NULL )
            {
                strncpy( bptr->from_class_name, tmp_c, CLS_NAME_SIZE );
            }

            tmp_c = NULL;
            EIM_find_value( headers, row->line, "to_uid", EIM_varchar, &tmp_c );
            if( tmp_c != NULL )
            {
                strncpy( bptr->to_uid, tmp_c, MAX_UID_SIZE );
            }

            tmp_i = NULL;
            EIM_find_value( headers, row->line, "to_class", EIM_integer, &tmp_i );
            if( tmp_i != NULL )
            {
                bptr->to_class = *tmp_i;
            }

            tmp_c = NULL;
            EIM_find_value( headers, row->line, "to_class_name", EIM_varchar, &tmp_c );
            if( tmp_c != NULL )
            {
                strncpy( bptr->to_class_name, tmp_c, CLS_NAME_SIZE );
            }

            tmp_i = NULL;
            EIM_find_value( headers, row->line, "bp_count", EIM_integer, &tmp_i );
            if( tmp_i != NULL ) 
            {
                bptr->bp_count = *tmp_i;
            }

            bptrs.push_back( *bptr );
            SM_free( (void *) bptr );
        }
    }

    EIM_free_result( headers, report );
    report = NULL;
    headers = NULL;

    if( !trans_was_active )
    {
        EIM_commit_transaction( "get_class_name()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out( msg );

    if( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return( ifail );
}

static int report_backpointers( std::vector<bp_t> &bptrs )
{
    int ifail = OK;
    int bp_cnt = bptrs.size();

    std::stringstream msg;
    msg << "Reporting on " << bp_cnt << " backpointer(s)";
    cons_out( msg.str() );

    for( int i=0; i < bp_cnt; i++ )
    {
        bp_t* ptr = &(bptrs[i]);
        std::stringstream msg;
        msg << "Backpointer " << (i+1) << ":";
        cons_out( msg.str() );

        msg.str("");
        msg << "    from_uid        = " << ptr->from_uid;
        cons_out( msg.str() );

        msg.str("");
        msg << "    from_class      = " << ptr->from_class;
        cons_out( msg.str() );

        msg.str("");
        msg << "    from_class_name = " << ptr->from_class_name;
        cons_out( msg.str() );

        msg.str("");
        msg << "    to_uid          = " << ptr->to_uid;
        cons_out( msg.str() );

        msg.str("");
        msg << "    to_class        = " << ptr->to_class;
        cons_out( msg.str() );

        msg.str("");
        msg << "    to_class_name   = " << ptr->to_class_name;
        cons_out( msg.str() );

        msg.str("");
        msg << "    bp_count        = " << ptr->bp_count;
        cons_out( msg.str() );

        cons_out( "" );
    }


    return( ifail );
}


static int report_object_refs( const std::string from_uid, int from_class, const std::string from_class_name, const std::string to_uid, int to_class, const std::string to_class_name, int ref_count, const std::string cmd_from_class, const std::string cmd_to_class )
{
    int ifail = OK;

    std::stringstream msg;
    msg << "Reporting on object references from " << from_uid << " to " << to_uid;
    cons_out( msg.str() );

    msg.str("");
    msg << "    from_uid        = " << from_uid;
    cons_out( msg.str() );

    msg.str("");
    msg << "    from_class      = " << from_class;
    cons_out( msg.str() );

    msg.str("");
    msg << "    from_class_name = " << from_class_name;
    cons_out( msg.str() );

    msg.str("");
    msg << "    to_uid          = " << to_uid;
    cons_out( msg.str() );

    msg.str("");
    msg << "    to_class        = " << to_class;
    cons_out( msg.str() );

    msg.str("");
    msg << "    to_class_name   = " << to_class_name;
    cons_out( msg.str() );

    msg.str("");
    msg << "    ref_count       = " << ref_count;
    cons_out( msg.str() );

    cons_out( "" );

    return( ifail );
}

#ifdef SLECHTA_NOT_NEEDED
/*-----------------------------------------------------------------*/
static std::string get_class_name( int cpid )
{
    std::string class_name;

    logical trans_was_active=true;
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "select pname from PPOM_CLASS where pcpid = " << cpid;

    EIM_select_col( &(vars[0]),  EIM_varchar, "pname",  CLS_NAME_SIZE+1,   false );
    int ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if( ifail == OK  && report != NULL )
    {
        char* tmp = NULL;
        EIM_find_value( headers, report->line, "pname", EIM_varchar, &tmp );
        class_name = tmp;
        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    if( !trans_was_active )
    {
        EIM_commit_transaction( "get_class_name()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out( msg );

    if( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return( class_name );
}
#endif

/*-----------------------------------------------------------------*/
static int get_cpid( const std::string class_name )
{
    int ret_cpid = -1;
    logical trans_was_active=true;
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;

    ERROR_PROTECT
    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "select pcpid from PPOM_CLASS where pname = '" << class_name << "'";

    EIM_select_col( &(vars[0]),  EIM_integer, "pcpid",  sizeof(int),   false );
    int ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if( ifail == OK  && report != NULL )
    {
        int* tmp = NULL;
        EIM_find_value( headers, report->line, "pcpid", EIM_integer, &tmp );

        if( tmp != NULL )
        {
            ret_cpid = *tmp;
        }
    }

    EIM_free_result( headers, report );
    report = NULL;
    headers = NULL;

    if( !trans_was_active )
    {
        EIM_commit_transaction( "get_cpid()" );
    }

    ERROR_RECOVER
    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out( msg );

    if( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code() );
        ERROR_raise( ERROR_line, EIM_ask_abort_code() ,"Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return( ret_cpid );
}


static int adjust_backpointers( const std::string from_uid, int from_cpid, const std::string to_uid, int to_cpid, int bp_count )
{
    int ifail = OK;
    logical trans_was_active=true;

    if( !EIM_is_transaction_active() )
    {
         trans_was_active = false;
         EIM_start_transaction();
    }

    std::stringstream sql;
    sql << "DELETE FROM POM_BACKPOINTER WHERE from_uid = '" << from_uid << "' AND to_uid = '" << to_uid << "'";
    ifail =  EIM_exec_imm( sql.str().c_str(), NULL );
    EIM_check_error ("adjust_backpointers - drop");

    if( ifail == OK && bp_count > 0 )
    {
        sql.str("");
        sql << "INSERT INTO POM_BACKPOINTER (from_uid, from_class, to_uid, to_class, bp_count) values ('" << from_uid << "', '" << from_cpid << "', '" << to_uid << "', '" << to_cpid << "', '" << bp_count << "')";
        ifail =  EIM_exec_imm( sql.str().c_str(), NULL );
        EIM_check_error ("adjust_backpointers - insert");
    }

    if( !trans_was_active )
    {
        EIM_commit_transaction( "adjust_backpointers()" );
    }

    return( ifail );
}

/*-----------------------------------------------------------------*/
static int getCmdLineArgs( int argc, char** argv, args_t *args )
{
   int i;
   int ret = OK;
   char* no_disp = NULL;

   logger()->printf("Command line: reference_manager");

    for (i=1 ; i < argc; i++)
    {
        if      (strcmp (argv[i],"-find_ref")       == 0) {args->op                  = find_ref;     }
        else if (strcmp (argv[i],"-find_ext_ref")   == 0) {args->op                  = find_ext_ref; }
        else if (strcmp (argv[i],"-find_class")     == 0) {args->op                  = find_class;   }
        else if (strcmp (argv[i],"-find_stub")      == 0) {args->op                  = find_stub;    }
        else if (strcmp (argv[i],"-check_ref")      == 0) {args->op                  = check_ref;    }       
        else if (strcmp (argv[i],"-load_obj")       == 0) {args->op                  = load_obj;     }
        else if (strcmp (argv[i],"-add_ref")        == 0) {args->op                  = add_ref;      }
        else if (strcmp (argv[i],"-remove_ref")     == 0) {args->op                  = remove_ref;   }
        else if (strcmp (argv[i],"-validate_bp")    == 0) {args->op                  = validate_bp;  }
        else if (strcmp(argv[i], "-validate_bp2")   == 0) { args->op                 = validate_bp2; }
        else if (strcmp (argv[i],"-correct_bp")     == 0) {args->op                  = correct_bp;   }
        else if (strcmp (argv[i],"-delete_obj")     == 0) {args->op                  = delete_obj;   }
        else if (strcmp(argv[i], "-str_len_val")    == 0) { args->op                 = str_len_val;  }
        else if (strcmp(argv[i], "-str_len_meta")   == 0) { args->op                 = str_len_meta; }
        else if (strcmp(argv[i], "-scan_vla")       == 0) { args->op                 = scan_vla;     }
        else if (strcmp(argv[i], "-remove_unneeded_bp") == 0) { args->op             = remove_unneeded_bp; }
        else if (strcmp(argv[i], "-edit_array" )    == 0) { args->op                 = edit_array; }
        else if (strcmp(argv[i], "-validate_cids") == 0)  { args->op                 = validate_cids; }
        else if (strcmp(argv[i], "-where_ref")      == 0) { args->op                 = where_ref; args->where_ref_sub = 1;     }
        else if (strcmp(argv[i], "-where_ref2")     == 0) { args->op                 = where_ref; args->where_ref_sub = 2;     }
        else if (strncmp(argv[i],"-u=",3)           == 0) {args->user_flag           = TRUE; args->username      = argv[i]+3;  }       
        else if (strncmp(argv[i],"-p=",3)           == 0) {args->pwd_flag            = TRUE; args->password      = argv[i]+3;  no_disp = "-p=<secret>";  }
        else if (strncmp(argv[i],"-g=",3)           == 0) {args->group_flag          = TRUE; args->usergroup     = argv[i]+3;  }
        else if (strncmp(argv[i],"-pf=",4)          == 0) {args->pwf_flag            = TRUE; args->pwf           = argv[i]+4;  no_disp = "-pf=<secret>";  } 
        else if (strncmp(argv[i],"-uid=",5)         == 0) {args->uid_flag            = TRUE; args->uid           = argv[i]+5; args->uid_vec->push_back(argv[i]+5); }
        else if (strncmp(argv[i],"-vc=",4)          == 0) {                                  args->val_class_n   = argv[i]+4;  }
        else if (strncmp(argv[i],"-c=",3)           == 0) {args->class_flag          = TRUE; args->class_n       = argv[i]+3;  }
        else if (strncmp(argv[i],"-a=",3)           == 0) {args->attribute_flag      = TRUE; args->attribute     = argv[i]+3;  }
        else if (strcmp(argv[i],"-i")               == 0) {args->ignore_errors_flag  = TRUE;                                   }
        else if (strcmp(argv[i],"-n")               == 0) {args->noparallel_flag     = TRUE;                                   } 
        else if (strncmp(argv[i],"-o=",3)           == 0) {args->class_obj_flag      = TRUE; args->class_obj_n   = argv[i]+3;  }  
        else if (strcmp(argv[i],"-v")               == 0) {args->verbose_flag        = TRUE;                                   } 
        else if (strcmp(argv[i],"-force")           == 0) {args->force_flag          = TRUE;                                   } 
        else if (strncmp(argv[i],"-from=", 6)       == 0) {args->from_flag           = TRUE; args->from          = argv[i]+6;  }
        else if (strncmp(argv[i],"-to=", 4)         == 0) {args->to_flag             = TRUE; args->to            = argv[i]+4;  }
        else if (strcmp(argv[i],"-debug")           == 0) {args->debug_flag          = TRUE;                                   }  
        else if (strcmp(argv[i],"-deleted")         == 0) {args->deleted_flag        = TRUE;                                   } 
        else if (strcmp(argv[i],"-null_ref")        == 0) {args->null_ref_flag       = TRUE;                                   } 
        else if (strcmp(argv[i],"-commit")          == 0) {args->commit_flag         = TRUE;                                   }
        else if (strcmp(argv[i],"-all")             == 0) {args->all_flag            = TRUE;                                   } 
        else if (strncmp(argv[i],"-lic_key=", 9)    == 0) {args->lic_key_flag        = TRUE; args->lic_key       = argv[i]+9;  } 
        else if (strncmp(argv[i],"-lic_file=", 10)  == 0) {args->lic_file_flag       = TRUE; args->lic_file      = argv[i]+10; } 
        else if (strcmp( argv[i], "-m")             == 0) {args->min_flag            = TRUE;                                   }  /* Process the option with minimum functionality */
        else if (strncmp(argv[i],"-aos=", 5)        == 0) {args->expected_error = atoi(argv[i]+5);                             }  /* Expected ifail value, a different value generates an error. */
        else if (strncmp(argv[i],"-cnt=", 5)        == 0) {args->target_cnt = atoi(argv[i]+5);                                 }  /* Expected finds, if number found differs then an error is generated.*/
        else if (strncmp(argv[i],"-max=", 5)        == 0) {args->max_ref_cnt = atoi(argv[i]+5);                                }  /* Maximum number of finds after which the utility terminates. (default = 100) */
        else if (strcmp(argv[i],"-h")               == 0) {args->help                = 1;                                      }  /* 1 = display detailed help information.*/
        else if (strcmp(argv[i],"-keep_system_log") == 0) {args->keep_system_log     = TRUE;                                   }
        else if (strcmp(argv[i],"-keep_logs" )      == 0) {args->keep_system_log     = TRUE;                                   }
        else if (strcmp(argv[i],"-alt")             == 0) {args->alt                 = TRUE;                                   }  /* true = alternate processing I.e. to_uid instead of from_uid */
        else if (strcmp(argv[i],"-log_details")     == 0) {args->log_details         = TRUE;                                   }  /* true = log additional details and keep syslog */       
        else if (strncmp(argv[i],"-f=", 3)          == 0) {args->file_name           = argv[i] + 3;                            }  /* File name from command line.*/
        else                                              {args->not_supported_flag  = TRUE; args->not_supported = argv[i]+0;   ret = FAIL; }

        if (no_disp != NULL)
        {
            logger()->printf(" %s", no_disp);
            no_disp = NULL;
        }
        else
        {
            logger()->printf(" %s", argv[i]);
        }
    }

    // Make sure user has not entered a max value less than 1.
    if ( args->max_ref_cnt < 1 )
    {
        args->max_ref_cnt = 100;
    }

    logger()->printf("\n");

#ifdef PRE_TC11_PLATFORM
    if( !args->pwd_flag && args->pwf_flag )
    {
        char *user = NULL;
        char *pw = NULL;
        char *grp = NULL;

        // Get the u, p (or pf), and g args.
        SSS_ask_login_args( argc, argv, -1, -1, -1, &user, &pw, &grp );

        if( args->password == NULL && pw != NULL )
        {
           // Move the password (from password file) into password memory location.
           args->password = pw;
           args->pwd_flag = TRUE;
           args->pwf_flag = FALSE;
        }
    }
#endif

    return( ret );
}

/*--------------------------------------------------*/
static int isValidOptionArgument(const args_t& args)
{
    int ret = OK;

    if( (args.user_flag          == FALSE) ||
        (args.group_flag         == FALSE) ||
         ( args.uid_flag == FALSE && ( args.op != add_ref && args.op != remove_ref && args.op != validate_bp && args.op != correct_bp && args.op != check_ref && args.op != str_len_val 
             && args.op != str_len_meta && args.op != scan_vla && args.op != remove_unneeded_bp && args.op != validate_bp2 && args.op != edit_array && args.op != validate_cids ) ) ||
        (args.class_obj_flag     == TRUE && (args.class_flag == TRUE || args.attribute_flag == TRUE)) ||
        (args.not_supported_flag == TRUE)
      ) 
    {
       ret = FAIL;
    }
    return( ret );
}

/*---------------------------------------------------*/
static int isPotentialPasswordAvailable(args_t& args)
{
    if(args.pwd_flag != FALSE && args.pwf_flag != FALSE)
    {
        const std::string msg( "Cannot specify -pf and -p on the command line" );
        cons_out( msg );
        return( FAIL );
    }
    else if(args.pwd_flag == FALSE && args.pwf_flag == FALSE)
    {
        const std::string msg( "Missing either -pf or -p on the command line" );
        cons_out( msg );
        return( FAIL );
    }

    if(args.pwd_flag != FALSE)
    {
        args.pwf = SM_string_copy(args.password); // get a copy of the actual password
    }

#ifndef PRE_TC11_PLATFORM
    else if(args.pwf_flag != FALSE)
    {
        char *pass = NULL;
        logical openSuccess = true;
        openSuccess = BASE_get_password_from_file(args.pwf, &pass );

        if (!openSuccess )
        {
            std::stringstream msg;
            msg << "Get password from file failed to open file: " << args.pwf << " or password is null or blank";
            cons_out( msg.str() );
            return( FAIL );
        }
        args.pwf = pass;
    }
#endif

    if(args.pwf == 0 || strlen(args.pwf) < 1)
    {
        const std::string msg( "NULL or empty password is not allowed" );
        cons_out( msg );
        return( FAIL );
    }
    return( OK );
}

/*-------------------------------*/
/* Find the root executable name */
static char* findExeRoot( char* exe)
{
    char* ptr = exe; 
    char* ret = NULL;
    while( *ptr != '\0') 
    {
        if( *ptr == '\\' || *ptr == '/')
        {
            ret = ptr+1;
        }
        ptr++;
    }

    if( ret == NULL) 
    {
        ret = exe;
    }

    return ret;
}


/* Display utility usage */
static void dspUsage( const char* exe )
{
    std::stringstream msg;
    msg << "Usage: ";

    if ( args->help > 0 )
    {
        msg << "\n  This tool provides a command line interface for working with Teamcenter references and low-level POM objects.";
        msg << "\n  There is no need to shut down Teamcenter while using this tool.";
        msg << "\n";
        msg << "\n  The primary functionality of this tool is to find, check, and correct Teamcenter references. This tool";
        msg << "\n  will also find an object's storage class, as well as perform load, lock, and delete testing of POM objects.";
        msg << "\n";
        msg << "\n  Note:";
        msg << "\n  1. In general class names should follow these rules, however some options are more forgiving than others. ";
        msg << "\n     With \"class:attribute:uid[:pos]\" syntax the class name should be the POM class where the attribute is defined.";
        msg << "\n     With \"class:uid\" syntax the class name should be the class of the object.";
        msg << "\n     When working with objects (-load_obj, -delete_obj) the class name should be the class of the object.";
        msg << "\n     When searching (-find_ref, find_ext_ref, find_class) the class name (-c and -o) is the starting class.";
    }

    msg << "\n";
    msg << "\n       " << exe << " -h (for detailed help)";
    msg << "\n  OR   " << exe << " -find_ref     -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid [-c=class] [-a=attribute] [-i] [-n] [-o=class] [-v] [-max=nnn]";
    msg << "\n  OR   " << exe << " -find_ext_ref -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid [-c=class] [-a=attribute] [-i] [-n] [-o=class] [-v] [-max=nnn]";
    msg << "\n  OR   " << exe << " -find_class   -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid [-uid=uid [...]]] [-c=class]";
    msg << "\n  OR   " << exe << " -find_stub    -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid [-uid=uid [...]]]";

    if ( args->help > 0 )
    {
        msg << "\n";
        msg << "\n           NOTE: The -find_xxxxx options can take a significant amount of time - be patient.";
        msg << "\n";
    }

    msg << "\n  OR   " << exe << " -check_ref    -u=user -p=pwd | -pf=pwdfile -g=group -from=class:uid -to=class:uid";
    msg << "\n  OR   " << exe << " -load_obj     -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid [-c=class] [-uid=uid [-uid=uid [...]]]  [-force]";
    msg << "\n  OR   " << exe << " -add_ref      -u=user -p=pwd | -pf=pwdfile -g=group -from=class:attribute:uid[:pos] -to=class:uid [-commit]";
    msg << "\n  OR   " << exe << " -remove_ref   -u=user -p=pwd | -pf=pwdfile -g=group -from=class:attribute:uid[:pos] -to=class:uid [-null-ref] [-all] [-commit]";
    msg << "\n  OR   " << exe << " -validate_bp  -u=user -p=pwd | -pf=pwdfile -g=group -from=class:uid -to=class:uid";
    msg << "\n  OR   " << exe << " -validate_bp2 -u=user -p=pwd | -pf=pwdfile -g=group [-c=class] [-log_details]";
    msg << "\n  OR   " << exe << " -correct_bp   -u=user -p=pwd | -pf=pwdfile -g=group -from=class:uid -to=class:uid [-commit]";
    msg << "\n  OR   " << exe << " -delete_obj   -u=user -p=pwd | -pf=pwdfile -g=group [-c=class] -uid=uid [-uid=uid [-uid=uid [...]]] [-commit]";
    msg << "\n  OR   " << exe << " -where_ref    -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid";
    msg << "\n  OR   " << exe << " -where_ref2   -u=user -p=pwd | -pf=pwdfile -g=group -uid=uid";
    msg << "\n  OR   " << exe << " -str_len_val  -u=user -p=pwd | -pf=pwdfile -g=group -from=class:attribute [-uid=uid [-uid=uid [...]]] [-commit]";
    msg << "\n  OR   " << exe << " -str_len_meta -u=user -p=pwd | -pf=pwdfile -g=group -c=class";
    msg << "\n  OR   " << exe << " -scan_vla     -u=user -p=pwd | -pf=pwdfile -g=group  [-c=class] [-a=attribute] [-uid=uid [-uid=uid [...]]] [-m]";
    msg << "\n  OR   " << exe << " -remove_unneeded_bp -u=user -p=pwd | -pf=pwdfile -g=group [-v] [-commit] [-uid=uid [-uid=uid [...]]]";
    msg << "\n  OR   " << exe << " -edit_array         -u=user -p=pwd | -pf=pwdfile -g=group -f=<csv_file> [-commit]";
    msg << "\n  OR   " << exe << " -validate_cids      -u=user -p=pwd | -pf=pwdfile -g=group -vc=<validation_class_name> [-m] [-max=nnn]";

    if ( args->help > 0 )
    {
        msg << "\n";
        msg << "\n -find_ref: search for typed and untyped references to the specified UID (Max output is 101)";
        msg << "\n   -uid=      UID to search for in reference attributes";
        msg << "\n   -a=        Attribute to be searched - can't be used with -o=";
        msg << "\n   -c=        Class to be searched - can't be used with -o=";
        msg << "\n   -o=        Search starting class (searches all classes up the hiearachy through POM_object)";
        msg << "\n   -i         Ignore errors when querying reference attributes";
        msg << "\n   -n         Remove parallel hint when querying reference attributes";
        msg << "\n   -v         Verbose output";
        msg << "\n   -max=      Maximum number of finds after which the utility terminates (default=100)";

        msg << "\n";
        msg << "\n -find_ext_ref: search for external references to the specified UID (Max output is 101)";
        msg << "\n   -uid=      UID to search for in external reference attributes";
        msg << "\n   -a=        Attribute to be searched - can't be used with -o=";
        msg << "\n   -c=        Class to be searched - can't be used with -o=";
        msg << "\n   -o=        Search starting class (searches all classes up the hiearachy through POM_object)";
        msg << "\n   -i         Ignore errors when querying reference attributes";
        msg << "\n   -n         Remove parallel hint when querying reference attributes";
        msg << "\n   -v         Verbose output";
        msg << "\n   -max=      Maximum number of finds after which the utility terminates (default=100)";

        msg << "\n";
        msg << "\n -find_class: find the defining class for the specified UIDs";
        msg << "\n   -c=        First class to search, if not found then POM_object and all flattened classes are searched";
        msg << "\n   -uid=      UID to search for in reference attributes";

        msg << "\n";
        msg << "\n -find_stub: find the stub object that represents the specified UID";
        msg << "\n   -uid=      UID to search for in the POM_stub:object_uid attribute";

        msg << "\n";
        msg << "\n -load_obj: Test if the object can be loaded, read-locked, modify-locked and delete-locked";
        msg << "\n   -c=        Class of UIDs to be loaded, default is to find the class in POM_object";
        msg << "\n   -uid=      UID to load and test locking operation";
        msg << "\n   -force     Force load the objects in the specified class";

        msg << "\n";
        msg << "\n -add_ref: Adds a reference to the from attribute that references the to object";
        msg << "\n           and updates the POM_BACKPOINTER table.";
        msg << "\n   -from=class:attribute:uid[:pos]  Location of the reference to be added.";
        msg << "\n   -to=class:uid                    Target referenced object.";
        msg << "\n   -commit                          commit the addition.  Default is to rollback the additions.";

        msg << "\n";
        msg << "\n -remove_ref: Removes the reference found in the from attribute";
        msg << "\n                                    and updates the POM_BACKPOINTER table.";
        msg << "\n   -from=class:attribute:uid[:pos]  Location of the reference to be removed.";
        msg << "\n   -to=class:uid                    Target referenced object.";
        msg << "\n   -null_ref                        use NULL reference rather than NULL value (usable only for nulls-allowed attrs.";
        msg << "\n   -all                             Either :pos or -all MUST be specified for multi-valued attributes.";
        msg << "\n   -commit                          commit the deletions.  Default is to rollback the deletes.";

        msg << "\n";
        msg << "\n -validate_bp: Compares actual object references to the contents of the POM_BACKPOINTER table";
        msg << "\n                    and then recommends subsequent actions based on the results";
        msg << "\n   -from=class:uid  Reference source (from) object";
        msg << "\n   -to=class:uid    Target (to) referenced object";
        msg << "\n   -deleted         User asserts the object has been deleted";

        msg << "\n";
        msg << "\n -validate_bp2: Validates backpointers on mass and writes problem information to the syslog";
        msg << "\n                    Validation includes 1) Incorrect from_class 2) Incorrect backpointer count";
        msg << "\n                                        3) Missing backpointers 4) Unneeded backpointers";
        msg << "\n   -c=class_name    Class to be validated, by default all classes are validated which can take hours";
        msg << "\n   -log_details     Identifies every problem backpointer in the syslog (Search for BPV:)";

        msg << "\n";
        msg << "\n -correct_bp: Adjusts the POM_BACKPOINTER entry to match actual object references";
        msg << "\n   -from=class:uid  Reference source (from) object";
        msg << "\n   -to=class:uid    Target (to) referenced object";
        msg << "\n   -deleted         User asserts the object has been deleted";
        msg << "\n   -commit          commit the corrections.  Default is to rollback corrections";

        msg << "\n";
        msg << "\n -delete_obj: Delete an object";
        msg << "\n   -c=        Optional class of object to delete";
        msg << "\n   -uid=      Uid of object to delete";
        msg << "\n   -commit    commit the deletions.  Default is to rollback the deletes";

        msg << "\n";
        msg << "\n -where_ref:  Find the records (in POM_BACKPOINTER & PIMANRELATION) referencing the target UID";
        msg << "\n   -uid=uid   UID to seach for in POM_BACKPOINTER & PIMANRELATION tables";

        msg << "\n";
        msg << "\n -where_ref2: Find the records (in all_backpointer_references view) referencing the target UID";
        msg << "\n   -uid=uid   UID to seach for in all_backpointer_references view";

        msg << "\n";
        msg << "\n -str_len_val: Validate or correct string-data or string-lengths associated with POM_string and POM_long_string attributes";
        msg << "\n   -from=class:attribute String attribute for which its data or string length is validated or corrected.";
        msg << "\n   -commit               For POM_long_strings update the string length, for POM_strings truncate an appropriate amount of data";
        msg << "\n   -uid=UID              Object for which the attributes length is validated, or corrected when -commit is specified";
        msg << "\n   Notes:     1. This option does NOT do object locking - do not use the \"-commit\" option with active users on the system.";
        msg << "\n              2. The -commit option can be used on an active system if a UID is specified for an object that will not load. (See -load_obj option)";

        msg << "\n";
        msg << "\n -str_len_meta: Display the reference_manager commands to validate attributes of a specified class and associated parent classes";
        msg << "\n   -c=class     The class for which reference_manager commands are displayed ";

        msg << "\n";
        msg << "\n -scan_vla:     Scans variable length arrays (VLAs) for data inconsistencies, by default scans all VLAs";
        msg << "\n   -c=class     The class name containing the VLAs to be scanned";
        msg << "\n   -a=attribute The attribute name of the VLA to be scanned";
        msg << "\n   -uid=UID     The object UID of the VLAs to be scanned";
        msg << "\n   -m           Minimum functionality eliminates the output of parallel VLA data.";

        msg << "\n";
        msg << "\n -remove_unneeded_bp: Removes backpointers where the from_uid value does not exist in the local database";
        msg << "\n   -v           Dumps backpointers that will be removed to the console. They are always logged to the system log";
        msg << "\n   -commit      Actually removes unneeded backpointers";
        msg << "\n   -uid=UID     Limits processing to the specified UID";
        msg << "\n   -alt         Process to_uids rather than from_uids - use only if instructed to do so by Siemens support";
        msg << "\n   Notes:       1. Always keep the syslog so you have documentation as to what was done";
        msg << "\n                2. Do NOT run in parallel sessions at the same time";
        msg << "\n                3. MUST be run with exclusive use of the database when running with the -commit option and without any -uid option";

        msg << "\n";
        msg << "\n -edit_array: Edits one or more array attributes based on contents of the input CSV file";
        msg << "\n   -commit      Commits edits - default behavior is rollback array edits";
        msg << "\n   -f=<file>    CSV file that directs edits to array attributes - file must be formatted as follows";
        msg << "\n                Header:  action,class,uid,pos,<attr1>,<attr2>,...<attrn>";
        msg << "\n                Data:    <action>,<class>,<uid>,<pos>,<attr_data1>,<attr_data2>,...<attr_datan>";
        msg << "\n   Notes:       1. Valid actions: insert, update, delete, append";
        msg << "\n                2. All data is case sensitive, including headers";
        msg << "\n                3. Supported array types: VLA (length = -1)";
        msg << "\n                4. Supported attribute types: POM_string, POM_int, POM_untyped_reference, POM_typed_reference";
        msg << "\n                5. Supported character sets: Single byte ASCII based and UTF-8";

        msg << "\n";
        msg << "\n -validate_cids: Validates class-ID values within reference attributes - logs corrective UPDATE SQL";
        msg << "\n                statements to the console and syslog (find corrective SQL: grep \"VCIDS:\" *.syslog)";
        msg << "\n   -vc=<class>  Validation class - used as the definitive source of object metadata. This must be POM_object";
        msg << "\n                or any flattened class. Running the utility without the -vc parameter will display a list of";
        msg << "\n                all acceptable classes. For complete coverage the utility must be run with all listed classes.";
        msg << "\n   -max=        Maximum number of finds after which the utility terminates (default=100)";
        msg << "\n   -m           Minimum functionality - don't log corrective UPDATE statements to console";

        msg << "\n";
        msg << "\n standard options:";
        msg << "\n   -u=         Teamcenter user ID";
        msg << "\n   -p=         Password for user ID";
        msg << "\n   -pf=        File containing password for user ID";
        msg << "\n   -g=         Teamcenter group";
        msg << "\n   -lic_key=   Licensing key for a specific option";
        msg << "\n   -lic_file=  File containing licensing key for a specific option";
        msg << "\n   -keep_system_log The system log file remains after utility has terminated";

        msg << "\n";
        msg << "\nDescription:";
        msg << "\n   -find_ref:     Searches all typed and untyped attributes for the specified UID. Search terminates";
        msg << "\n                  when 101 references are found or all typed and untyped attributes have been searched";
        msg << "\n   -find_ext_ref: Searches all external refernence attributes for the specified UID. Search terminates";
        msg << "\n                  when 101 references are found or all external reference attributes have been searched";
        msg << "\n   -find_class:   Identifies the defining class by first searching the target class (-c option)";
        msg << "\n                  and then continuing to search the POM_object class and all flattened classes,";
        msg << "\n                  assuming the defining class has not been identified";
        msg << "\n   -check_ref:    Loads both the referenecing and referenced objects and also validates the contents of";
        msg << "\n                  the POM_BACKPOINTER table associated with references between the two objects";
        msg << "\n   -load_obj:     Attempts to load the specified UID, read-lock, modify-lock, delete-lock and unload the object";
        msg << "\n   -add_ref:      Sets a reference in the from object's attribute to the target object";
        msg << "\n   -remove_ref:   Removes a reference between two objects";
        msg << "\n   -delete_obj:   Delete objects, objects are not deleted if any are referenced.";
        msg << "\n   -validate_bp:  Compares the actual number of references with references found in the POM_BACKPOINTER table";
        msg << "\n   -correct_bp:   Sets the number of references in the POM_BACKPOINTER table to the actual number of references";
        msg << "\n   -where_ref:    Displays objects (from BOM_BACKPOINTER & PIMANRELATION) referencing the target object";
        msg << "\n   -where_ref2:   Find the records (in all_backpointer_references view) referencing the target UID";
        msg << "\n   -str_len_val:  Validate or correct the strings (or string lengths) of POM_string and POM_long_string attributes";
        msg << "\n   -str_len_meta: Display Ref-Mgr commands to validate attributes for the specified class";
        msg << "\n   -scan_vla:     Scan VLAs looking for inconsistencies between the VLA sequence values and the VLA record count stored on the class table";
        msg << "\n   -remove_unneeded_bp: Removes POM_BACKPOINTER records that don't point to valid objects or stubs";
        msg << "\n   -edit_array:         Edits one or more arrays based on the CSV input file";
        msg << "\n   -validate_cids:      Searches for references with invalid class IDs and creates corrective SQL";
  
    }
    msg << "\n";
    msg << "\n Examples:";
    msg << "\n   1. Find a the class of an object, inspecting a minimal number of classes for ";
    msg << "\n      complete database coverage (not including PPOM_STUB.POBJECT_UID).";
    msg << "\n         reference_manager -find_class -u=  -p=  -g=  -uid=QRX56eboAAgcRA";
    msg << "\n";
    msg << "\n   2. Find a reference to the specified UID, searching ALL typed and untyped references.";
    msg << "\n         reference_manager -find_ref -u=  -p=  -g=  -uid=goPAAAAIAAgcBA";
    msg << "\n";
    msg << "\n   3. Find a reference to the specified UID, searching typed and untyped references,";
    msg << "\n      starting with the ItemRevision class and walking up the hiearchy to the POM_object class.";
    msg << "\n         reference_manager -find_ref -u=  -p=  -g=  -uid=QRX56eboAAgcRA -o=ItemRevision";
    msg << "\n";
    msg << "\n   4. Load an object (-c= option is no longer required for classes with an entry in the PPOM_OBJECT table).";
    msg << "\n         reference_manager -load_obj -u=  -p=  -g=  -uid=QVZ56eboAAgcRA";
    msg << "\n";
    msg << "\n   5. Delete an object (Object is NOT deleted if it is referenced).";
    msg << "\n         reference_manager -delete_obj -u=  -p=  -g=  -lic_file=file.txt -c=PSConnectionRevision";
    msg << "\n                                                                         -uid=QVZ56eboAAgcRA -commit";
    msg << "\n";
    msg << "\n   6. Scan all VLAs for any inconsistency.";
    msg << "\n         reference_manager -scan_vla -u=  -p=  -g=  ";
    msg << "\n";
    msg << "\n Version = " << REF_MGR_VERSION;


    cons_out( msg.str() );
}

/* Console output that also goes to the syslog. */
static void cons_out( const std::string msg )
{
    std::string msg2 = msg + "\n";
    fnd_printf( msg2.c_str() );
    std::string cons( "Cons: " );
    cons += msg2;
    logger()->printf( cons.c_str() );
}

/* Console output witn no logging. */
static void cons_out_no_log( const std::string msg )
{
    std::string msg2 = msg + "\n";
    fnd_printf( msg2.c_str() );
}


/* Find the root file name */
static const char* find_root_file( const char *file_name )
{
    const char * ptr  = file_name;
    const char * ptr1 = NULL;
    const char * ptr2 = NULL;

    while( (ptr = strchr( ptr, '\\')) != NULL )
    {
        ptr++;
        ptr1 = ptr;
    }

    if( ptr1 != NULL )
    {
        return( ptr1 );
    }

    ptr = file_name;

    while( (ptr = strchr( ptr, '/')) != NULL )
    {
        ptr++;
        ptr1 = ptr;
    }

    if( ptr2 != NULL )
    {
        return( ptr2 );
    }

    return( file_name );
}


/* Console output that also goes to the syslog. */
static int error_out( const char *file_name, int line_number, int failure_code, const std::string msg )
{

    int ret_code = failure_code;
    const char* root = NULL;

    if( ret_code != OK )
    {
        root = find_root_file( file_name );

        std::stringstream out;
        out << "ERROR (" << ret_code << "): " << msg; 

        std::string o_msg = out.str();
        o_msg += "\n";
        fnd_printf( o_msg.c_str() );  // Output to console

        out << " [" << root << " (" << line_number << ")]\n";

        std::string cons( "Cons: " );
        cons += out.str();
        logger()->printf( cons.c_str() ); // Output to log file.
    }

    return( ret_code );
}


#define REF1 "query_class_to_find_class_of_uid" 


static int query_class_to_find_class_of_uid ( std::vector< std::string > uids, const std::string target_class, std::map< std::string, std::string > &found_classes )
{
    static const char * pc_tbl            = "PPOM_CLASS";
    static const int    pc_col_cnt        = 2;
    static const char * pc_col_names[]    = { "pname", "pcpid" };
    static const int    pc_col_types[]    = { DDS_string, DDS_integer };
    static const int    pc_col_lens[]     = { 33, sizeof(int) };
    static const char *select_attr_list[] = { "pname" };
    static const char *target_attr_list[] = { "uid_as_uid" };
    static const char *select_puid_list[] = { "puid" };
    static logical table_registered = FALSE;
    std::string ret_class;

    int          uid_cnt  = uids.size();
    const char **uids_ptr = (const char**) SM_alloc( sizeof( const char*) * (uid_cnt + 1));
    uids_ptr[0] = NULL;

    for( int i=0; i<uids.size(); i++ )
    {
        uids_ptr[i] = uids[i].c_str();
        uids_ptr[i+1] = NULL;
    }   

    int n_rows = 0;
    int n_cols = 0;
    void*** report = NULL;
    int ifail = POM_ok;

    ifail = POM_enquiry_create( REF1 );

    try
    {
// #ifndef PRE_TC11_PLATFORM
#if !defined(PRE_TC11_PLATFORM) 
#if !defined(PRE_TC11_2_3_PLATFORM)
        if( ifail == POM_ok )         POM__enquiry_suppress_version_configuration( REF1 );
#else
        if( ifail == POM_ok )         POM__enquiry_suppress_revision_configuration( REF1 );
#endif
#endif
        /*
        ** We need to use a HACK and register the PPOM_CLASS table as a normal table. 
        ** The ENQ system gets confused (has a problem) when the target class is "POM_class" 
        ** and we join in an aliased POM_class - in this case zero records were returned. :-(
        */
        if( !table_registered )
        {
            if( ifail == POM_ok ) ifail = POM_enquiry_register_table( pc_tbl, pc_col_cnt, pc_col_names, pc_col_types, pc_col_lens );
            if( ifail == POM_ok ) table_registered = TRUE;
        }
        if( ifail == POM_ok ) ifail = POM_enquiry_create_class_alias ( REF1, "PPOM_CLASS", 0, "POM_class_alias" );
        if( ifail == POM_ok ) ifail = POM_enquiry_set_attr_expr      ( REF1, "uid_as_uid", target_class.c_str(), "puid", POM_enquiry_uid_of, "" );
        if( ifail == POM_ok ) ifail = POM_enquiry_add_select_exprs   ( REF1, 1, target_attr_list );
        if( ifail == POM_ok ) ifail = POM_enquiry_add_select_attrs   ( REF1, "POM_class_alias", 1, select_attr_list );
        if( ifail == POM_ok ) ifail = POM_enquiry_add_select_attrs   ( REF1, target_class.c_str(), 1, select_puid_list );   /* This is required so that ENQ caches the CPID associated with each the instance */
        if( ifail == POM_ok ) ifail = POM_enquiry_set_join_expr      ( REF1, "where_join_expr", target_class.c_str(), "pid", POM_enquiry_equal, "POM_class_alias", "pcpid" );
        if( ifail == POM_ok ) ifail = POM_enquiry_set_string_value   ( REF1, "where_expr_uid_values",   uid_cnt, uids_ptr, POM_enquiry_const_value );
        if( ifail == POM_ok ) ifail = POM_enquiry_set_attr_expr      ( REF1, "where_expr_target_class", target_class.c_str(), "puid", POM_enquiry_in, "where_expr_uid_values" );
        if( ifail == POM_ok ) ifail = POM_enquiry_set_expr           ( REF1, "where_expression", "where_join_expr", POM_enquiry_and, "where_expr_target_class" );
        if( ifail == POM_ok ) ifail = POM_enquiry_set_where_expr     ( REF1, "where_expression" );
#if !defined(PRE_TC133_PLATFORM)
        if( ifail == POM_ok ) ifail = POM_enquiry_exec               ( REF1, &n_rows,  &n_cols, &report );
#else
        ERROR You need to comment out this line and uncomment the next line. (Not allowed to submit with calls to POM_enquiry_execute.)
        // if( ifail == POM_ok ) ifail = POM_enquiry_execute            ( REF1, &n_rows,  &n_cols, &report );
#endif

        if ( report != 0 && ifail == POM_ok )
        {
            for( int i=0; i<n_rows; i++ )
            {
                std::map< std::string, std::string >::iterator it;
                it = found_classes.find( (const char*) report[i][0] );
                if( it != found_classes.end() )
                {
                    found_classes.erase( it );
                }

                found_classes.insert( std::pair< std::string, std::string >( (char *)report[i][0], (char *)report[i][1] ));
            }
        }
    }
    catch (...)
    {
        ifail = POM_internal_error;
    }

    if( report != NULL )
    {
        SM_free( report );
    }

    POM_enquiry_delete( REF1 );

    SM_free( (void *)uids_ptr);

    return( ifail );
}

static int query_class_to_find_class_of_uid (  const std::string uid, const std::string target_class, std::string  &found_class )
{
    int ifail = OK;
    std::vector< std::string > uids;
    std::map< std::string, std::string > found_classes;

    uids.push_back( uid );

    ifail = query_class_to_find_class_of_uid( uids, target_class, found_classes );

    if( ifail == OK )
    {

        std::map< std::string, std::string >::iterator it;

        it = found_classes.find( uid );

        if( it != found_classes.end() )
        {
            found_class = it->second;
        }
    }

    return( ifail );
}

/*----------------------------------------------------------------*/
// Return the class name associated with the specified CPID.
// In valid CPID or error returns an empty string.
static std::string get_class_name_from_cpid( std::string cpid )
{
    std::string ret_val = "";
    int ifail = POM_ok;

    std::string::size_type sz = 0;
    int l_cpid = -1;

    try
    {
        l_cpid = std::stoi( cpid, &sz );
    }
    catch (...)
    {
        l_cpid = -1;   
    }  

    if ( l_cpid < 2 || sz != cpid.length( ) )
    {
        return ret_val;
    }
 
    EIM_select_var_t vars[1];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;

    std::stringstream sql;
    sql << "SELECT pname FROM PPOM_CLASS WHERE pcpid  = " << l_cpid; 
    EIM_select_col( &(vars[0]), EIM_varchar, "pname", 35, false );
    ifail = EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 1, vars, 0, NULL );

    if ( ifail == POM_ok )
    {
        if( report != NULL )
        {
            char *pname = NULL;
            EIM_find_value (headers, report->line, "pname", EIM_char, &pname);

            if ( pname != NULL )
            {
                ret_val = pname;
            }
        }
    }
    else
    {
        EIM_clear_error( );
    }

    EIM_free_result( headers, report );
    return ret_val;
}

/*----------------------------------------------------------------*/
// This function will read all the flattened-class IDs (cpids) 
// and names from the pom data dicitionay and populate the 
// flattened_class_map. 
static int get_flattened_class_names( std::vector< std::string > &flattened_classes )
{
    int ret_val = POM_ok;
     
    EIM_select_var_t vars[3];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    std::stringstream sql;
    sql << "SELECT pcpid, pname, pproperties FROM PPOM_CLASS WHERE pproperties >= " << FLATTENED_CLASS_PROPERTY; 
    EIM_select_col( &(vars[0]), EIM_integer, "pcpid", sizeof(int), false );
    EIM_select_col( &(vars[1]), EIM_varchar, "pname", 35, false );
    EIM_select_col( &(vars[2]), EIM_integer, "pproperties", sizeof(int), false );
    EIM_exec_sql_bind( sql.str().c_str(), &headers, &report, 0, 3, vars, 0, NULL );
    EIM_check_error( "get_flattened_class_names" );

    if( report != NULL )
    {
        int  row_cnt = 0;
        int  *pcpid = NULL;
        char *pname = NULL;
        int  *properties = NULL;

        for (row = report; row != NULL; row = row->next) row_cnt++;

        // Read each of the flattened class name and cache their values.
        if( row_cnt > 0)
        { 

            for (row = report; row != NULL; row = row->next)
            {
                EIM_find_value (headers, row->line, "pcpid", EIM_integer, &pcpid);
                EIM_find_value (headers, row->line, "pname", EIM_char, &pname);
                EIM_find_value (headers, row->line, "pproperties", EIM_integer, &properties);

                if( (*properties & FLATTENED_CLASS_PROPERTY) == FLATTENED_CLASS_PROPERTY)
                {
                    flattened_classes.push_back( pname );
                }
            }
        }
    }

    EIM_free_result( headers, report );
    return ret_val;
}

/* 
Find the original strings which are not keys in the map. 
The map is expected to contain the results from some operation (such as a query)
that answers a request for a given input set (original_vec). This routine
identifies the set of original strings that are not keys in the map.
*/
static int find_unresolved( std::vector< std::string > &original_vec, std::map< std::string, std::string > &result_map, std::vector< std::string > &unresolved_vec )
{
    int ifail = POM_ok;

    std::map< std::string, std::string >::iterator it;

    for( int i = 0; i < original_vec.size(); i++ )
    {
        it = result_map.find( original_vec[i] );

        if( it == result_map.end() )
        {
            unresolved_vec.push_back( original_vec[i] );
        }
    }

    return( ifail );
}

/* 
Find the original strings which are not keys in the map or are not of the specified class. 
The map is expected to contain the results from some operation (such as a query) that
answers a request for a given input set (original_vec). This routine identifies the set of 
original strings that are not keys in the map or do not match the specified class.
*/
static int find_unresolved( const std::string class_name, std::vector< std::string > &original_vec, std::map< std::string, std::string > &result_map, std::vector<  std::string > &unresolved_vec )
{
    int ifail = POM_ok;

    std::map< std::string, std::string >::iterator it;

    for( int i = 0; i < original_vec.size(); i++ )
    {
        it = result_map.find( original_vec[i] );

        if( it == result_map.end() )
        {
            unresolved_vec.push_back( original_vec[i] );
        }
        else
        {
            if( class_name.compare( it->second ) != 0 )
            {
                unresolved_vec.push_back( original_vec[i] );
            }
        }
    }

    return( ifail );
}

/*
Refresh the specified object requesting a RIL lock type. 
Returns true if there were no problems. False if there are any problems.
*/
static logical refreshToLock( tag_t tag, int pom_lock, tag_t class_tag )
{
    logical ret_val = FALSE;

    ERROR_PROTECT

//        POM_refresh_instances( 1, &tag, class_tag, POM_no_lock );

        POM_refresh_instances( 1, &tag, class_tag, pom_lock );

        ret_val = TRUE;

    ERROR_RECOVER
    ERROR_END

    return( ret_val );
}

/*
Load the specified object to a no-lock state. 
Returns true if there were no problems. False if there are any problems.
*/
static logical loadToNoLock( tag_t tag, tag_t class_tag )
{
    logical ret_val = FALSE;

    unsigned int num_cached = 0;
    char* class_name = NULL;
    int ifail = POM_name_of_class(class_tag, &class_name);

    if (ifail == POM_ok && class_name != NULL && class_name[0] != '\0')
    {
        ifail = POM_cache_cpids_of_class(1, &tag, class_name, NULL, &num_cached);
    }

    if (num_cached == 0 || ifail != POM_ok)
    {
        return(ret_val);
    }

    ERROR_PROTECT

        int ifail = POM_load_instances( 1, &tag, class_tag, POM_no_lock );

        if (ifail == POM_ok)
        {
            ret_val = TRUE;
        }

    ERROR_RECOVER
    ERROR_END

    return( ret_val );
}

/*
Unload the specified object. 
Returns true if there were no problems. False if there are any problems.
*/
static logical unload( tag_t tag )
{
    logical ret_val = FALSE;

    ERROR_PROTECT

        POM_unload_instances( 1, &tag );

        ret_val = TRUE;

    ERROR_RECOVER
    ERROR_END

    return( ret_val );
}

/*
Extract a sub-parameter from either the -to= or the -from= parameters.
*/
static std::string getSubParameter( const char* param, int sub_param_pos )
{
    std::string ret_value;
    char* ptr = SM_string_copy( param );
    char* str_pos = ptr;
    char* end_pos = NULL;
    int cur_pos = 0;

    // Find sub parameter starting location.
    while( cur_pos < sub_param_pos && str_pos != NULL )
    {
        str_pos = strchr( str_pos, ':' );

        if( str_pos != NULL )
        {
            str_pos++;
        }
        cur_pos++;
    }

    if( str_pos == NULL )
    {
        SM_free( ptr );
        return( ret_value );
    }

    end_pos = strchr( str_pos, ':' );

    if( end_pos != NULL )
    {
        *end_pos = '\0';
    }

    ret_value = str_pos;

    SM_free( ptr );

    return( ret_value );
}


/*
Start a working transaction.  
A working transaction is a transaction that has a better than normal
chance of being rolled back. I.e. it is NOT a transaction that is purely
for the purpose of allowing queries to be executed. A working transaction 
typically includes DML that will possibly be rolled back. 

RETURN Value: 0 (OK)

*/
static const char* trans_id = NULL;

#ifndef PRE_TC11_REF_MGR
static logical active_tx = FALSE;
#endif

static int start_working_tx( const char* tx_id )
{
    if( trans_id != NULL )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Unable to start working tx %s as tx %s is currently active.", tx_id, trans_id);
    }

    if( strlen( tx_id ) > 32 )
    {
        // This limitation is associated with the SAVEPOINT length constraints.
        ERROR_raise( ERROR_line, POM_internal_error, "Transaction ID %s must be shorter than 23 characters.", tx_id );
    }

    trans_id = tx_id;

#ifndef PRE_TC11_REF_MGR
    active_tx = EIM_is_transaction_active();

    if( active_tx) 
    {
        // Terminate standard transaction
        EIM_commit_transaction( tx_id );
    }
#else
    logical tx = EIM_is_transaction_active();

    if( !tx )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "POM transaction MUST be active before calling start_working_tx().\n" );
    }

    if ( EIM_dbplat() == EIM_dbplat_oracle || EIM_dbplat() == EIM_dbplat_postgres )
    {
        std::stringstream sql;
        sql.str("");
        sql << "SAVEPOINT " << tx_id;
        EIM_exec_imm( sql.str().c_str(), NULL );       
    }
    else if ( EIM_dbplat() == EIM_dbplat_mssql )
    {
        std::stringstream sql;
        sql.str("");
        sql << "SAVE TRANSACTION " << tx_id;
        EIM_exec_imm( sql.str().c_str(), NULL );  
    }
    else
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Unsupported database platform." );
    }

    EIM_check_error ("start_working_tx() - SAVEPOINT generation.");
#endif

    return( OK );
}

static int commit_working_tx( )
{
    if( trans_id == NULL )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Transaction has NOT been started.");
    }

#ifndef PRE_TC11_REF_MGR
    if( active_tx )
    {
        // Start a traditional transaction
        EIM_start_transaction();
    }
#else
    trans_id = NULL;
#endif

    return( OK );
}


static int rollback_working_tx( )
{
    if( trans_id == NULL )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Transaction has NOT been started.");
    }

#ifndef PRE_TC11_REF_MGR
    if( active_tx )
    {
        // Start a traditional transaction
        EIM_start_transaction();
    }
#else
    if ( EIM_dbplat() == EIM_dbplat_oracle || EIM_dbplat() == EIM_dbplat_postgres )
    {
        std::stringstream sql;
        sql.str("");
        sql << "ROLLBACK TO SAVEPOINT " << trans_id;
        EIM_exec_imm( sql.str().c_str(), NULL );       
    }
    else if ( EIM_dbplat() == EIM_dbplat_mssql )
    {
        std::stringstream sql;
        sql.str("");
        sql << "ROLLBACK TRANSACTION " << trans_id;
        EIM_exec_imm( sql.str().c_str(), NULL );  
    }
    else
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Unsupported database platform." );
    }

    EIM_check_error ("rollback_working_tx() - SAVEPOINT restore.");
#endif
    trans_id = NULL;

    return( OK );
}


// #ifdef PRE_TC11_PLATFORM
#if defined(PRE_TC11_PLATFORM) || defined(LOCAL_CACHE_CPIDS)
/*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
/**
    Add class IDs to the cpid cache for instances of the specified class.
    <br/>
    If an instance does not have a cpid registered with the cpid cache, the specified class is searched
    and if the instance is found the class ID is then registered with the cpid cache. This routine will
    cache the class Id of any object associated with the specified class or its subclasses provided a
    record exists in the class table of the specified class - this is the normal class inheritance model.
    <br/>
    A database round trip is incurred only if one or more instances are
    found not to have class IDs registered with the cpid cache.
    <br/>
    This API is used to ensure an object's class ID is registered with the cpid cache.
    This is especially usefull after a class has been converted into a lightweight object.

    @returns
   <ul>
    <li>#POM_ok on success
    <li>Otherwise an error code from the creation and execution of the query.
   </ul>
*/
static int DMS_cache_cpids_of_class(
    const unsigned int  n_tags,      /**< (I) Number of instances to search for in the specified class */
    const tag_t         *instances,  /**< (I) n_tags The instances to search for in the specified class */
    const char          *class_name, /**< (I) Name of the class to search - optional, NULL can be specified and no classes are searched */
    unsigned int        *prior_reg,  /**< (O) The number of instances registered prior to searching the class - optional, NULL can be specified */
    unsigned int        *final_reg   /**< (O) The number of instances registered after searching the class table - optional, NULL can be specified */
    )
{
    int           ifail    = POM_ok;
    int           n_target = 0;
    EIM_uid_t     *target  = NULL;
    unsigned int  loop     = 0;
    int           cpid     = DDS_null_class_pid;
    std::vector<const char *> idVec;

    if( prior_reg != NULL )
    {
        *prior_reg = 0;
    }

    if( final_reg != NULL )
    {
        *final_reg = 0;
    }

    if( n_tags < 1 )
    {
        return ifail;
    }

    target = (EIM_uid_t *)SM_alloc( sizeof(EIM_uid_t) * (n_tags+1) );
    idVec.clear();

    for( loop=0, n_target=0; loop<n_tags; loop++ )
    {
        cpid = DMS_get_cpid_from_cache( instances[loop] );

        if( cpid != DDS_null_class_pid )
        {
            if( prior_reg != NULL )
            {
                (*prior_reg)++;
            }
        }
        else
        {
            EIM_uid_of_tag( instances[loop], target[n_target]);
            idVec.push_back( target[n_target] );
            n_target++;
        }
    }

    if( class_name != NULL && n_target > 0 )
    {
        int     n_rows=0;
        int     n_cols=0;
        void*** report = NULL;

        // Query the specified class table for the specified instances
        const char *queryName = "dms_cache_cpids_of_class";
        const char *select_attrs[2] = { "puid", "pid" };
        if( ifail == POM_ok ) ifail = POM_enquiry_create( queryName );
        if( ifail == POM_ok ) ifail = POM_enquiry_add_select_attrs( queryName, class_name, 2, select_attrs );                                      /* select the puid & pid columns to be output */
        if( ifail == POM_ok ) ifail = POM_enquiry_set_attr_expr( queryName, "puid_as_uid", class_name, "puid", POM_enquiry_uid_of, "" );           /* expression that indicates we are searching on uid of tag */
        if( ifail == POM_ok ) ifail = POM_enquiry_set_string_value( queryName, "target_uids", idVec.size(), &(idVec[0]), POM_enquiry_bind_value ); /* list of target uids to be searched for */
        if( ifail == POM_ok ) ifail = POM_enquiry_set_expr( queryName, "where_expr", "puid_as_uid",  POM_enquiry_in, "target_uids" );              /* Look for all uids that are in the list of specified uids */
        if( ifail == POM_ok ) ifail = POM_enquiry_set_where_expr( queryName, "where_expr" );                                                       /* add the where clause to the query */

        if( ifail != POM_ok )
        {
            ERROR_raise (ERROR_line, POM_internal_error, "Unable to create query %s, error code = %d", queryName, ifail );
        }

        ifail = POM_enquiry_exec ( queryName, &n_rows, &n_cols, &report );
        POM_enquiry_delete ( queryName );

        if( ifail != POM_ok )
        {
            ERROR_raise (ERROR_line, POM_internal_error, "Unable to execute query %s, error code = %d", queryName, ifail );
        }

        for ( int i=0 ; i < n_rows ; ++i )
        {
            tag_t tmp_tag = *((tag_t *)report[ i ][ 0 ]);
            int classId   = *((int *)report[ i ][ 1 ]);

            // The ENQ system might have already cached the cpid,
            // so we'll check first before registering the class ID.
            cpid = DMS_get_cpid_from_cache( tmp_tag );

            if( cpid == DDS_null_class_pid )
            {
                DMS_record_cpid( tmp_tag, classId );
            }
        }

        SM_free( report );
    }

    SM_free( target );

    if( final_reg != NULL )
    {
        for( loop=0, n_target=0; loop<n_tags; loop++ )
        {
            cpid = DMS_get_cpid_from_cache( instances[loop] );

            if( cpid != DDS_null_class_pid )
            {
                (*final_reg)++;
            }
        }
    }
    return( ifail );
}
#endif

static logical is_digit( char ch )
{
    logical ret = FALSE;

    if( ch >= '0' && ch <= '9' )
    {
        ret = TRUE;
    }

    return( ret );
}

/* ********************************************************************************
** Utility licensing routines. 
** *******************************************************************************/

/* Does the license key specified on the command line grant access to the 
** option specified in the function parameter. 
** A return value: OK = access granted, any other value = access denied.
*/
static int licenseFor( const char* option )
{
    int ifail = OK; 
    std::string lic_key;
    char* pom_param_value = NULL;

    pom_param_value = DDS_ask_pom_parameter("Ref_Mgr_Lic_key");

    // Error if neither license file or license key was specified
    if( !args->lic_key_flag && !args->lic_file_flag  && pom_param_value == NULL )
    {
        std::stringstream msg;
        msg << "\n Option " << option << " requires:";
        msg << "\n         1) A license key be specified (see -lic_key= or -lic_file=)";
        msg << "\n            -OR-";
        msg << "\n         2) install -set_pom_param -u= -p= -g= Ref_Mgr_Lic_key <lic_key>";
        cons_out( msg.str() );
        ifail = POM_invalid_value;
        return( ifail );
    }

    if( args->lic_file_flag == TRUE )
    {
        // Get license key from license file.
        std::vector< unsigned char >  lic_chars = readStringFromFile( args->lic_file, false );

        for( int i=0; i<lic_chars.size() && lic_chars[i] != '\0'; i++ )
        {
            lic_key += lic_chars[i];
        }
    }
    else if( args->lic_key_flag == TRUE && strlen( args->lic_key) > 0 )
    {
        // Get license key from command line.
        lic_key = args->lic_key;
    }
    else if (pom_param_value != NULL)
    {
        lic_key = pom_param_value;
    }      

    if( lic_key.size() < 1 )
    {
        std::stringstream msg;
        msg << "\n License key is missing or has a length of zero";
        cons_out( msg.str() );
        ifail = POM_invalid_value;
        return( ifail );
    }


    unsigned char* lic = NULL;

    licensing_start();
    lic = BASE_decrypt( (const unsigned char*) lic_key.c_str() );              
    licensing_stop();

    if( lic == NULL )
    {
        ifail = POM_invalid_value;
    }

    if( ifail == OK )
    {

        int field_cnt   = 0;
        int data_size   = 0;
        char *cp        = NULL;
        char *data      = NULL;
        char **ptr      = NULL;
        char *field_mem = NULL;
        char *start     = NULL;

        // Count the number of encrypted fields
        cp = (char*) lic;
        data_size = strlen( cp );
        cp = strchr( cp, ',' );

        while( cp != NULL )
        {
            field_cnt++;
            cp = strchr( cp+1, ',' );
        }

        // Allocate memory, we are goinging to need:
        // memory for all the data (data_size)
        // memory for a pointer to each field, 
        //       plus the last field which does not have a comma
        //       plus a terminating NULL pointer 
        // memory for terminating NULL character for all fields. 
        field_mem = (char *) SM_calloc( 1, data_size + (sizeof(void*) * (field_cnt+2)) + (field_cnt+1) );
        ptr       = (char**) field_mem;
        data      = (char*)  (ptr + field_cnt + 2);
        cp        = (char*)  lic;
        cp--;

        while( cp != NULL )
        {
            start = cp + 1;
            cp = strchr( start, ',' );

            if( cp != NULL )
            {
                strncpy( data, start, (cp-start));
                data[(cp-start)] = '\0';
            }
            else
            {
                strcpy( data, start );
            }
            *ptr = data;
            ptr++;
            *ptr = NULL;

            data = data + (strlen(data) + 1);
        }

        SM_free( (void*) lic );

        ptr = (char**) field_mem;

        //
        // Validate content of each of the fields.
        //

        //
        // Skip over version field. 
        //
        ptr++;


        //
        // Validate utility name
        //
        if( ifail == OK && strcasecmp( "reference_manager", *ptr ) != 0 )
        {
            ifail = POM_invalid_value;
        }
        ptr++;

        //
        // Validate date
        //
        if( ifail == OK )
        {
            int year = 0;
            int month = 0;
            int day = 0;

            char *cp = *ptr;

            // Get Year
            if( is_digit( *cp ) )
            {
                year = atoi( cp );

                while( is_digit( *cp ) )
                {
                    cp++;
                }

                // skip over '/'
                if( *cp != '\0' )
                {
                    cp++;
                }
            }
            else
            {
                ifail = POM_invalid_value;
            }

            // Get Month
            if( ifail == OK && is_digit( *cp ) )
            {
                month = atoi( cp );

                while( is_digit( *cp ) )
                {
                    cp++;
                }

                // skip over '/'
                if( *cp != '\0' )
                {
                    cp++;
                }
            }
            else if( ifail == OK )
            {
                ifail = POM_invalid_value;
            }

            // Get day
            if( ifail == OK && is_digit( *cp ) )
            {
                day = atoi( cp );
            }
            else if( ifail == OK )
            {
                ifail = POM_invalid_value;
            }

            if( ifail == OK )
            {
                time_t now = time( NULL );
                struct tm * timeinfo;

                timeinfo = localtime ( &now );

                timeinfo->tm_year = year - 1900;
                timeinfo->tm_mon  = month - 1;
                timeinfo->tm_mday = day;
                timeinfo->tm_hour = 0;
                timeinfo->tm_min  = 0;
                timeinfo->tm_sec  = 0;

                time_t lic_date = mktime( timeinfo );

                int week_secs = 7*24*60*60;

                if( lic_date > (now + week_secs) || lic_date < (now - week_secs) )
                {
                    std::stringstream msg;
                    msg << "\n >>>> " << args->lic_key << " <<<< License key is expired";
                    cons_out( msg.str() );
                    ifail = POM_invalid_value;
                }
            }        
        }
        ptr++;

        //
        // validate option
        //
        if( ifail == OK && ( strcasecmp( "-all", *ptr ) != 0  && strcasecmp( option, *ptr ) != 0) )
        {
            ifail = POM_invalid_value;
        }

        SM_free( (void*) field_mem );
    }

    if( ifail != OK )
    {
        std::stringstream msg;
        msg << "\n >>>> " << args->lic_key << " <<<< Invalid license key specified";
        cons_out( msg.str() );
    }

    return( ifail );
 }

static void licensing_start()
{
    _context = NULL;
    _isInitialized = false;
    _iv = NULL;
    _ivLength = 0;
//    _logger = Teamcenter::FoundationBase::logger();
    _logger = logger();


    logical successStatus = false;
    TcCryptoSystemType systemType = TcCrypto_SystemType_Domestic;

    // Any failures during initialization will be cleaned up in the destructor.
    // Also, any TcCrypto initialization failures will get reported to the log
    // file and _context will remain NULL.  That NULL _context will then cause
    // failures in the public class methods.
    _isInitialized = ( 0 != TcCrypto_System_Initialize( systemType ) );
    if( _isInitialized )
    {
        std::vector<unsigned char> key;
        std::string keyFileName = Teamcenter::OSEnvironment::get("TC_DATA");   // Presumes TC_DATA is defined.
        keyFileName.append( _alternateKeyFile );
        std::vector<unsigned char> alternateKey = readStringFromFile( keyFileName.c_str(), true );
        if( alternateKey.size() < MAX_KEY_LENGTH )
        {    // Initialize with the default encryption key
            for( std::string::size_type i = 0; i != _defaultKey.size(); i++ )
            {
                key.push_back( static_cast<unsigned char>(_defaultKey[i]) );
            }
            _logger->info( ERROR_line, "Default encryption configuration." );
        }
        else
        {     // Clamp the length of the alternate key at 32.
            alternateKey.resize( MAX_KEY_LENGTH );
            key = alternateKey;
            _logger->info( ERROR_line, "Alternate encryption key used." );
        }

        if( setKey( key ) )
        {
            _context = TcCrypto_Cipher_GetContext( TCCRYPTO_CIPHER_TYPE_aes_256_cbc );
            if( NULL != _context )
            {
                successStatus = true;
            }
        }
    }
    if( !successStatus )
    {   // A failure here is a big deal, because the Crypto facility won't work.  Log it as fatal.
        // All the possible errors are TcCrypto, so the particular failure should be evident from the message.
        PrintErrors( ERROR_line, __FUNCTION__, Teamcenter::Logging::Logger::LOG_FATAL );
    }

}

static void licensing_stop()
{
    if( NULL != _iv )
    {
        delete _iv;
        _iv = NULL;
    }
    if( NULL != _context )
    {
        TcCrypto_System_FreeContext( _context );
        _context = NULL;
    }
    if( _isInitialized )
    {
        TcCrypto_System_Shutdown();
    }
}


static logical setKey( const std::vector<unsigned char> key )
{
    logical successStatus = false;
    TcCryptoContext* context = TcCrypto_Digest_GetContext( TCCRYPTO_DIGEST_TYPE_sha256 );
    if( NULL != context )
    {
        TcCrypto_Digest_Init( context );
        if( TcCrypto_Digest_Update( context, &key[0], key.size() ) )
        {
            unsigned char md_value[MAX_MD_SIZE];
#ifdef TCCRYPTO_VERSION_GE
#if TCCRYPTO_VERSION_GE(4,0)
            size_t md_len = MAX_MD_SIZE;
#else
            unsigned int md_len = MAX_MD_SIZE;
#endif
#else
            unsigned int md_len = MAX_MD_SIZE;
#endif

            if( TcCrypto_Digest_Final( context, md_value, &md_len ) )
            {
                // Use 32 bytes of the hash
                // The key value has to be at least 32 bits long for a 256 bit cipher.
                unsigned int i, j;
                for( i = 0, j = 0; i < sizeof(_secretKey ); i++, j++ )
                {
                    if( j > md_len )
                    {    // This is in case md_len is < 32.
                        j = 0;
                    }
                    _secretKey[i] = md_value[j];
                }
                successStatus = true;
            }
        }
        TcCrypto_System_FreeContext( context );
    }
    return successStatus;
}


static unsigned char* BASE_decrypt( const unsigned char* base64CryptText )
{
    _logger->trace( ERROR_line, "Begin %s.", __FUNCTION__ );

    if( NULL == base64CryptText )
    {
        std::stringstream msg;
        msg << "Crypt text input parameter is NULL.";
        _logger->warn( ERROR_line, 0, msg.str() );
        return NULL;
    }

    logical successStatus = false;
    unsigned char* cryptText;
    unsigned int cryptTextLen;

    fromBase64( base64CryptText, &cryptText, &cryptTextLen );

#ifdef TCCRYPTO_VERSION_GE
#if TCCRYPTO_VERSION_GE(4,0)
    size_t plainTextLen = cryptTextLen + 1;
#else
    int plainTextLen = cryptTextLen + 1;
#endif
#else
    int plainTextLen = cryptTextLen + 1;
#endif
    unsigned char* plainText = static_cast<unsigned char *>( SM_alloc(plainTextLen) );

    TcCrypto_Cipher_Init( _context, _secretKey, sizeof(_secretKey), _iv, _ivLength, 0 );
    TcCrypto_Cipher_SetPadding( _context, 1 );
    if( TcCrypto_Cipher_Update( _context, plainText, &plainTextLen, cryptText, cryptTextLen ) )
    {
        if( TcCrypto_Cipher_Final( _context, &plainText[plainTextLen], &plainTextLen ) )
        {
            successStatus = true;
        }
    }

    if( successStatus )
    {
        // Finally, strip off the leading date in the cleartext
        memmove( plainText, &plainText[sizeof( time_t )], cryptTextLen - sizeof( time_t ) );
    }
    else
    {
        PrintErrors( ERROR_line, __FUNCTION__, Teamcenter::Logging::Logger::LOG_WARN );
        SM_free( (void *) plainText );
        plainText = NULL;
    }

    SM_free( (void *) cryptText );
    _logger->trace( ERROR_line, "Normal end %s.", __FUNCTION__ );
    return plainText;
}


// Allocate a buffer for the encrypted string and do the base64 decoding.
// It's the caller's job to eventually free that memory.
static void fromBase64( const unsigned char* base64String, unsigned char** cryptText, unsigned int* cryptTextLen )
{
    unsigned int base64Length = strlen( (char *) const_cast<unsigned char *>(base64String) );
    unsigned int inLength = 0;
    *cryptText = static_cast<unsigned char *>(SM_alloc( base64Length ) );
    unsigned char* outPtr = *cryptText;
    unsigned char inBuff[4];

    while( base64Length - inLength > 3 )
    {
        for( int i = 0; i != 4; i++ )
        {
            inBuff[i] = _base64DecodeTable[base64String[inLength++]];
        }

        *outPtr++ = ((inBuff[0] << 2) + ((inBuff[1] & 0x30) >> 4));
        *outPtr++ = (((inBuff[1] & 0xf) << 4) + ((inBuff[2] & 0x3c) >> 2));
        *outPtr++ = (((inBuff[2] & 0x3) << 6) + inBuff[3]);
    }

    unsigned char outBuff[3];
    unsigned int remaining = base64Length - inLength;
    if( remaining > 0 )
    {
        inBuff[1] = inBuff[2] = inBuff[3] = '\0';
        for( unsigned int j = 0; j < remaining; j++ )
        {
            inBuff[j] = _base64DecodeTable[base64String[inLength++]];
        }

        outBuff[0] = (inBuff[0] << 2) + ((inBuff[1] & 0x30) >> 4);
        outBuff[1] = ((inBuff[1] & 0xf) << 4) + ((inBuff[2] & 0x3c) >> 2);
        outBuff[2] = ((inBuff[2] & 0x3) << 6) + inBuff[3];

        for( unsigned int j = 0; j < (remaining - 1); j++ )
        {
            *outPtr++ = outBuff[j];
        }
    }
    *cryptTextLen = static_cast<unsigned int> (outPtr - *cryptText);
}


// Open a file and read a vector.
static std::vector<unsigned char> readStringFromFile( const char* fName, bool silent )
{
    std::vector<unsigned char> inString;
    FILE *fp = fnd_fopen( fName, "r" );

    if ( 0 == fp )
    {
        // This is not necessarily an error.  Could just be an inaccurate pathname.
        if( !silent )
        {
            std::stringstream msg;
            msg << "Unable to open " << fName << " for reading.";
            _logger->warn( ERROR_line, 0, msg.str() );
        }
        return inString;
    }

    // String must be only contents of file - ie. no newline/carriage return accepted. Otherwise
    // would have to insist upon UTF8 and provide locale to UTF8 gen passfile util... So, keep it simple
    // and require locale content and no parsing.

    int c = getc(fp);

    while( EOF != c && '\n' != c && '\r' != c )
    {
        inString.push_back( (unsigned char) c );

        if( MAX_BASE64_LENGTH == inString.size() )
        {
            // An 8K input string for an encrypted password is just not plausible.
            fclose(fp);
            std::stringstream msg;
            msg << "String length > " << MAX_BASE64_LENGTH << ".";
            _logger->error( ERROR_line, 0, msg.str() );
            inString.clear();
            return inString;
        }
        c = getc(fp);
    }

    fclose(fp);
    inString.push_back( '\0' );

    return inString;
}

static const void PrintErrors( const char *file, const int line, const char *method, const Teamcenter::Logging::Logger::level lvl )
{
    char errorMessage[1024];
    int error = TcCrypto_System_GetLastError( errorMessage, sizeof( errorMessage ) );
    if( error )
    {
        //  print error message to the log file.
        std::stringstream msg;
        msg << method << " TcCrypto error \"" << errorMessage << "\".";
        _logger->log( lvl, file, line, 0, msg.str() );
    }
}
/* ********************************************************************************
** END OF: Utility licensing routines. 
** *******************************************************************************/

/* ********************************************************************************
** This sections contains routines for validating string lengths 
** on MS SQL Server and Oracle, primarily with Teamcenter running in UTF-8 mode.
**
** START OF: str_len_val_op() routines.
** *******************************************************************************/
static const char* db_char_len_fun()
{
    const char* ret = NULL;

    if ( EIM_dbplat() == EIM_dbplat_mssql )
    {
        ret = "LEN";
    }
    else if ( EIM_dbplat() == EIM_dbplat_oracle )
    {
        ret = "LENGTH";
    }
    else if ( EIM_dbplat() == EIM_dbplat_postgres )
    {
        ERROR_raise( ERROR_line, POM_op_not_supported, "Postgres is not currently supported with the -str_len_val option" );
    }
    return ret;
}

static const char* db_byte_len_fun()
{
    const char* ret = NULL;

    if ( EIM_dbplat() == EIM_dbplat_mssql )
    {
        ret = "DATALENGTH";
    }
    else if ( EIM_dbplat() == EIM_dbplat_oracle )
    {
        ret = "LENGTHB";
    }
    else if ( EIM_dbplat() == EIM_dbplat_postgres )
    {
        ERROR_raise( ERROR_line, POM_op_not_supported, "Postgres is not currently supported with the -str_len_val option" );
    }
    return ret;
}

static const char* db_substr_fun()
{
    const char* ret = NULL;

    if ( EIM_dbplat() == EIM_dbplat_mssql )
    {
        ret = "SUBSTRING";
    }
    else if ( EIM_dbplat() == EIM_dbplat_oracle )
    {
        ret = "SUBSTR";
    }
    else if ( EIM_dbplat() == EIM_dbplat_postgres )
    {
        ERROR_raise( ERROR_line, POM_op_not_supported, "Postgres is not currently supported with the -str_len_val option" );
    }
    return ret;
}

/*------------------------------------------------------------------------
** Given a class name and attribute name return the storage location
** of associated columns on the class table and on the attribute table.
** ----------------------------------------------------------------------- */
static int get_tables_and_columns(const char *cls_name, const char* att_name, const char** cls_tbl, const char** cls_col, const char** att_tbl, const char** att_col)
{
    int ifail = POM_ok;
    OM_class_t  cls_id = OM_null_c;
    DDS_class_p_t dclass = NULL;
    const char* c_tbl = NULL;
    int class_cpid = 0;

    OM_attribute_t att_id = OM_null_attribute;
    DDS_attribute_p_t datt = NULL;
    int a_dds_type = -1;
    int a_dds_len = -1;
    int att_apid = 0;

    if (cls_name == NULL) { return(POM_invalid_class_id); }
    if (att_name == NULL) { return(POM_invalid_attr_id); }

    cls_id = OM_lookup_class(cls_name);
    if (cls_id <= OM_invalid_c) { return(POM_invalid_class_id); }
    if (!OM_has_class_property(cls_id, OM_class_prop_pom_stored)) { return(POM_invalid_class_id); }

    c_tbl = DDS_table_name(cls_id);
    if (c_tbl == NULL || strlen(c_tbl) < 1) { return(POM_invalid_class_id); }

    dclass = (DDS_class_p_t)OM_ask_class_of_class_id(cls_id);
    if (dclass == NULL) { return(POM_invalid_class_id); }
    class_cpid = dclass->pid;

    att_id = OM_lookup_attribute(att_name);
    if (att_id == OM_null_attribute) { return(POM_invalid_attr_id); }

    datt = (DDS_attribute_p_t)OM_ask_attribute_p_ne((cls_id), (att_id));
    if (datt == NULL) { return(POM_invalid_attr_id); }
    if (!(datt->isa == DDS_attribute_c || OM_is_subclass(datt->isa, DDS_attribute_c))) { return(POM_invalid_attr_id); }
    if (OM_has_attribute_property(dclass->id, datt->id, OM_attr_prop_transient) || OM_has_attribute_property(dclass->id, datt->id, OM_attr_prop_classvar)) { return(POM_invalid_attr_id); }
    att_apid = datt->pid;

    // Currently only support long strings.
    a_dds_type = datt->ptype;
    if (a_dds_type != DDS_long_string && a_dds_type != DDS_string) { return(POM_op_not_supported); }

    // Currently only support scalars.
    a_dds_len = DDS_att_len(datt);
    // if (a_dds_len != 1 && a_dds_len != -1 && (a_dds_len <= 6 && a_dds_type != DDS_long_string)) { return(POM_op_not_supported); }

    // Ok, let's figure out the table and column names.
    switch (a_dds_type)
    {
    case DDS_long_string:
        if (a_dds_len == 1)
        {
            // POM_long_string scalar
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = SM_sprintf("%s_%d_%d", DDS_long_string_base, dclass->pid, datt->pid);
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("%s", "pval");
            }
        }
        else if (a_dds_len == -1)
        {
            // POM_long_string VLA
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = SM_sprintf("%s_%d_%d", DDS_long_string_base, dclass->pid, datt->pid);
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("%s", "pval");
            }
        }
        else if (a_dds_len > 6)
        {
            // POM_long_string LA (large array)
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = NULL;
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("%s", "pval");
            }
        }
        else if (a_dds_len <= 6)
        {
            // POM_long_string SA (small array)
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = NULL;
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("%s", "pval");
            }
        }
        break;

    case DDS_string:
        if (a_dds_len == 1)
        {
            // POM_string VLA.
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_tbl != NULL)
            {
                *att_tbl = NULL;
            }

            if (att_col != NULL)
            {
                *att_col = NULL;
            }
        }
        else if (a_dds_len == -1)
        {
            // POM_string VLA.
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = SM_sprintf("VLA_%d_%d", class_cpid, att_apid);
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("pval_0");
            }
        }
        else if (a_dds_len > 6)
        {
            // POM_string LA (large array).
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = NULL;
            }

            if (att_tbl != NULL)
            {
                *att_tbl = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_col != NULL)
            {
                *att_col = SM_sprintf("pval");
            }
        }
        else if (a_dds_len <= 6)
        {
            // POM_string SA (small array).
            if (cls_tbl != NULL)
            {
                *cls_tbl = SM_sprintf("%s", c_tbl);
            }

            if (cls_col != NULL)
            {
                *cls_col = SM_sprintf("%s", DDS_ask_dbname((DDS_attribute_p_t)OM_ask_attribute_p(cls_id, att_id)));
            }

            if (att_tbl != NULL)
            {
                *att_tbl = NULL;
            }

            if (att_col != NULL)
            {
                *att_col = NULL;
            }
        }
        break;
        default:
            ERROR_raise(ERROR_line, POM_internal_error, "get_tables_and_columns() detected an unsupported attribute type or array size.");
            break;
    }
    return(ifail);
}

/*------------------------------------------------------------------------
** Get the class table name for the specified class.
** ----------------------------------------------------------------------- */
static int get_class_table_name( const char* cls_name, const char** cls_tbl )
{
    int ifail = POM_ok;
    OM_class_t cls_id = OM_null_c;
    const char* c_tbl = NULL;

    if ( cls_name == NULL )
    {
        return ( POM_invalid_class_id );
    }

    cls_id = OM_lookup_class( cls_name );

    if ( cls_id <= OM_invalid_c )
    {
        return ( POM_invalid_class_id );
    }

    if ( !OM_has_class_property( cls_id, OM_class_prop_pom_stored ) )
    {
        return ( POM_invalid_class_id );
    }

    c_tbl = DDS_table_name( cls_id );

    if ( c_tbl == NULL || strlen( c_tbl ) < 1 )
    {
        return ( POM_invalid_class_id );
    }
 
    // Return the table name.
    if ( cls_tbl != NULL )
    {
        *cls_tbl = SM_sprintf( "%s", c_tbl );
    }

    return ( ifail );
}

/*------------------------------------------------------------------------
** Gets the class' storage mode. 
** The standard storage modes are "dual", "classic", and "flat".
** If the name is NOT an actual class we'll assume it ia a table
** and return "table".
** ----------------------------------------------------------------------- */
static std::string get_storage_mode( const char* cls_name )
{
    std::string ret = "table";
    int ifail = get_class_table_name( cls_name, NULL );

#if !defined(PRE_TC12_PLATFORM)
    if( !ifail )
    {
        int sm = -1;
        POM_ask_storage_mode_by_class( cls_name, &sm );
        ret = DMS_storage_mode_to_text( sm );   
    }
#else
    if( !ifail )
    {
        ret = "classic";
    }
#endif

    return ret;
}

#if defined(PRE_TC12_PLATFORM)
/*------------------------------------------------------------------------
** Gets the class' top query table. Prior to Tc12 the top query class
** is also the top class (top storage class).
** ----------------------------------------------------------------------- */
static std::string get_top_query_table( int cid )
{
    int ifail = POM_ok;
    OM_class_t cls_id = OM_null_c;
    const char* cls_name = NULL;

    std::string cls = "POM_object";
    std::string ret = "PPOM_OBJECT";

    ERROR_PROTECT
    cls_id = DDS_class_id_of_pid( cid );

    if ( cls_id <= OM_invalid_c )
    {
        ifail = POM_invalid_class_id;
    }

    if( !ifail && !OM_has_class_property( cls_id, OM_class_prop_pom_stored ) ) 
    {
        ifail = POM_invalid_class_id;
    }

    if( !ifail && OM_has_class_property( cls_id, OM_class_prop_has_flat_tables ))
    {
        cls_name = OM_ask_class_name( cls_id );

        if ( cls_name == NULL || strlen( cls_name ) < 1 )
        {
            ifail = POM_invalid_class_id;
        }
        else 
        {
            cls = cls_name;
        }
    }

    if( !ifail )
    {
        OM_class_t om_class_id = OM_lookup_class(cls.c_str());

        if (om_class_id <= OM_invalid_c )
        {
            ifail = POM_invalid_class_id;
        }
        else
        {
            DDS_class_p_t dclass1 = DDS_class_of_class_id(om_class_id);
            ret = DDS_ask_tname(dclass1);
        }
    }

    ERROR_RECOVER
    ifail = POM_invalid_class_id;
    ERROR_END

    if ( ifail )
    {
        std::stringstream msg;
        msg << "get_top_query_table(): CPID " << cid << " generated ifail=" << ifail << ".";
        logger()->warn( ERROR_line, 0, msg.str() );
        cons_out( msg.str() );
        ERROR_raise(ERROR_line, ifail, "get_top_query_table(): Invalid class ID was specified (%d)", cid);
    }

    return ret;
}
#endif

/* --------------------------------------------------------------------
** Output the reference manager commands for each of the supported
** attributes on the target class and the parent classes.
** ----------------------------------------------------------------- */
static int get_string_meta(const char* cls, const char** super)
{
    int ifail = OK;
    logical trans_was_active = true;
    EIM_select_var_t vars[8];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    trans_was_active = true;

    if (!EIM_is_transaction_active())
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    // Build SQL to retrieve meta from the database
    std::stringstream sql;

    sql << "SELECT a.pname cname, a.psuperclass, b.pname aname, a.pcpid, b.papid, b.pptype, b.pmax_string_length, b.plength AS array FROM PPOM_CLASS a, PPOM_ATTRIBUTE b WHERE b.rdefining_classu = a.puid AND a.pname = '";
    sql << cls << "' ORDER BY b.papid";

    EIM_select_col(&(vars[0]), EIM_varchar, "cname", 34, false);
    EIM_select_col(&(vars[1]), EIM_varchar, "psuperclass", 34, false);
    EIM_select_col(&(vars[2]), EIM_varchar, "aname", 34, false);
    EIM_select_col(&(vars[3]), EIM_integer, "pcpid", sizeof(int), false);
    EIM_select_col(&(vars[4]), EIM_integer, "papid", sizeof(int), false);
    EIM_select_col(&(vars[5]), EIM_integer, "pptype", sizeof(int), false);
    EIM_select_col(&(vars[6]), EIM_integer, "pmax_string_length", sizeof(int), false);
    EIM_select_col(&(vars[7]), EIM_integer, "array", sizeof(int), false);

    ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 8, vars, 0, NULL);
    EIM_check_error("Retrieving string meta\n");

    if (report != NULL)
    {
        int pos = -1;

        for (row = report; row != NULL; row = row->next)
        {
            pos++;

            char* cname = NULL;
            EIM_find_value(headers, row->line, "cname", EIM_varchar, &cname);

            char* superclass = NULL;
            EIM_find_value(headers, row->line, "psuperclass", EIM_varchar, &superclass);

            char* aname = NULL;
            EIM_find_value(headers, row->line, "aname", EIM_varchar, &aname);

            int* pcpid = NULL;
            EIM_find_value(headers, row->line, "pcpid", EIM_integer, &pcpid);

            int* papid = NULL;
            EIM_find_value(headers, row->line, "papid", EIM_integer, &papid);

            int* pptype = NULL;
            EIM_find_value(headers, row->line, "pptype", EIM_integer, &pptype);

            int* max_len = NULL;
            EIM_find_value(headers, row->line, "pmax_string_length", EIM_integer, &max_len);

            int* arry = NULL;
            EIM_find_value(headers, row->line, "array", EIM_integer, &arry);

            if (pos == 0)
            {
                cons_out(" ");
                std::stringstream msg;
                msg << "Class:";
                if (cname != NULL) { msg << cname; }
                else { msg << "null"; }
                if (pcpid != NULL) { msg << "(" << *pcpid << ")"; }
                else { msg << "(null)"; }

                if (cname != NULL && strcasecmp(cname, "POM_object") != 0)
                {
                    if (superclass != NULL)
                    {
                        msg << " Superclass:" << superclass;

                        if (super != NULL)
                        {
                            *super = (char*)SM_sprintf("%s", superclass);
                        }
                    }
                }
                cons_out(msg.str());
            }

            {
                std::stringstream msg;
                if (aname != NULL) { msg << aname; }
                else { msg << "null"; }
                if (papid != NULL) { msg << "(" << *papid << ")"; }
                if (pptype != NULL) { msg << " type=" << *pptype; }
                else { msg << " type=null"; }
                if (max_len != NULL) { msg << " max_len=" << *max_len; }
                else { msg << " max_len=null"; }
                if (arry != NULL) { msg << " array=" << *arry << ","; }
                else { msg << " array=null,"; }
               
                //
                // Do NOT output the -str_len_val commands when it is not supported.
                //
                if (EIM_dbplat() == EIM_dbplat_mssql || (EIM_dbplat() == EIM_dbplat_oracle && EIM_get_db_cs() != TEXT_CODESET_UTF8 ))
                {
                    if ( cname != NULL && aname != NULL && pptype != NULL && arry != NULL )
                    {
                        if ( ( *arry == 1 || *arry == -1 || *arry > 6 || *arry <= 6 ) && ( *pptype == 112 || *pptype == 117 ) )
                        {
                            msg << "\nreference_manager -str_len_val -u=user -p=password -g=dba -from=" << cname << ":" << aname;
                        }
                    }
                }

                cons_out(msg.str());
            }
        }
    }
    else
    {
        cons_out("No data for class was found.");
    }

    ERROR_RECOVER

        const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

        return(ifail);
}

/*------------------------------------------------------------------------
** Output progress information to the console.
** ----------------------------------------------------------------------- */
static void str_len_val_status(logical force, const char* state, const char* pre, int records, int worked, int under, int over, int equal, int unknown, int eligible, int corrected)
{
    if (!force)
    {
        // TODO: If force is not specified then we should output the information every few minutes, for now we'll ignore this additional information and just return.
        return;
    }

    std::stringstream msg;

    if (state != NULL)
    {
        msg << state;
    }
    else
    {
        msg << "      ";
    }

    if (pre != NULL)
    {
        msg << pre << ": ";
    }
    else
    {
        msg << "        ";
    }

    msg << "Records=" << records << " Worked=" << worked << " Under=" << under << " Over=" << over << " Equal=" << equal << " Unknown=" << unknown << " Bad=" << eligible << " Corrected=" << corrected;

    if (force)
    {
        cons_out(msg.str());
    }
}

/*------------------------------------------------------------------------
** Retrieves the calcualted sizes for POM_string attributes.
** ----------------------------------------------------------------------- */
static int get_string_sizes(const char *cls_tbl, const char *cls_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &calc_sizes)
{
    int ifail = OK;

    if ( EIM_dbplat() != EIM_dbplat_mssql && EIM_dbplat() != EIM_dbplat_oracle )
    {
        ERROR_raise(ERROR_line, POM_internal_error, "Functionality is not supported for RDBMSs other than MS SQL Server and Oracle.");
    }

    int  rm_slv_max_bytes_per_char = 4;      // UTF-8 we use 4. Used to calculate minimum length before we consider it a possible problem. 
    rm_slv_max_bytes_per_char = DDS_ask_pom_parameter_int("rm_slv_max_bytes_per_char", rm_slv_max_bytes_per_char);

    int storge_bytes_per_ascii_char = 2;    // For unicode there are two bytes for every ascii character

    if (!EIM_unicode_enabled())
    {
        storge_bytes_per_ascii_char = 1;    // If not unicode we assume UTF-8, in which case we have 1 storage byte per ascii character
    }

    puids.clear();
    calc_sizes.clear();

    logical trans_was_active = true;
    EIM_select_var_t vars[2];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

    std::stringstream sql;
    logical where_added = false;

    sql << "SELECT a.puid puid, " << db_byte_len_fun() << "(a." << cls_col << ") calc_size";
    sql << " FROM " << cls_tbl << " a";

    if ( rm_slv_max_bytes_per_char > 0 )
    {
        sql << " WHERE " << db_byte_len_fun() << "(a." << cls_col << ") >= " << (int)( ( max_size * storge_bytes_per_ascii_char ) / rm_slv_max_bytes_per_char );
        where_added = true;
    }

    if (uid != NULL || (uid_vec != NULL && uid_vec->size() > 0))
    {
        if (!where_added)
        {
            sql << " WHERE a.puid IN (";
            where_added = true;
        }
        else
        {
            sql << " AND a.puid IN (";
        }

        int inc_cnt = 0;

        if (uid != NULL)
        {
            sql << "'" << uid << "'";
            inc_cnt++;
        }

        if (uid_vec != NULL && uid_vec->size() > 0)
        {
            for (int i = 0; i < uid_vec->size(); i++)
            {
                if (inc_cnt > 0)
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec)[i] << "'";
                inc_cnt++;
            }
        }
        sql << ")";
    }

    sql << " ORDER BY calc_size ASC";

    EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
    EIM_select_col(&(vars[1]), EIM_integer, "calc_size", sizeof(int), false);

    ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 2, vars, 0, NULL);
    EIM_check_error("Retrieving string sizes\n");

    if (report != NULL)
    {
        for (row = report; row != NULL; row = row->next)
        {
            char* tmp_puid = NULL;
            EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);
            puids.push_back(tmp_puid);

            int* tmp_size = NULL;
            EIM_find_value(headers, row->line, "calc_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                calc_sizes.push_back(*tmp_size);
            }
            else
            {
                calc_sizes.push_back(0);
            }
        }
    }
    EIM_free_result(headers, report);
    report = NULL;
    headers = NULL;

    if (!trans_was_active)
    {
        EIM_commit_transaction("get_string_sizes()");
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return(ifail);
}

/*------------------------------------------------------------------------
** Validate the size of data in a POM_string attribute against
** the maximum size of the field.
** We'll assume that if the stored data length is less than 1/4 of the max data length
** that it will happily fit within the max buffer size when read from the database.
** ----------------------------------------------------------------------- */
static int validate_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = OK;
    // RM_SLV_ stands for Reference Manager String Length Validation
    int rm_slv_batch_size = 65500;       // Number of records processed per batch
    int rm_slv_max_len = 100000;         // Maximum length of records within the batch
    int rm_slv_percent_exp = 30;         // Percentage of expected expansion due to none single byte characters
    int rm_slv_fixed_exp = 100;          // Additional fixed byte expansion to provide additional memory for unforseen expansions...
    int rm_slv_max_mem_m = 6000;         // Maximum memory to be used within each batch... unit single record processing kicks in.
    int64_t rm_slv_meg = 1024 * 1024;    // Megabyte

    int rpt_worked_cnt = 0;              // Number of records that were processed
    int rpt_success_cnt = 0;             // Number of records where data length matches the stored length
    int rpt_under_cnt = 0;               // Number of records where the stored count under represents the data length
    int rpt_over_cnt = 0;                // Number of records where the stored count over represents the data length
    int rpt_unknown_cnt = 0;             // Number of records that we don't know about (NULL for size returned etc...)
    int rpt_eligible_cnt = 0;            // Number of records that are eligible for correction
    int rpt_corrected_cnt = 0;           // Number of records where the stored count was actually corrected

    int wrk_start_pos = 0;               // UID to start with.
    int wrk_max_len = 0;                 // Maximum size of a buffer that is expected to retrieve data
    int wrk_batch_size = 0;              // Batch size for the current batch. 

                                         // Check configuration values. 
    rm_slv_batch_size = DDS_ask_pom_parameter_int("rm_slv_batch_size", rm_slv_batch_size);
    rm_slv_max_len = DDS_ask_pom_parameter_int("rm_slv_max_len", rm_slv_max_len);
    rm_slv_percent_exp = DDS_ask_pom_parameter_int("rm_slv_percent_exp", rm_slv_percent_exp);
    rm_slv_fixed_exp = DDS_ask_pom_parameter_int("rm_slv_fixed_exp", rm_slv_fixed_exp);
    rm_slv_max_mem_m = DDS_ask_pom_parameter_int("rm_slv_max_mem_m", rm_slv_max_mem_m);
    EIM_uid_t* cor_puids = NULL;
    int*       cor_lens = NULL;


    // As we are currently not using ENQ or temporary tables we will limit the batch size to 400 to limit the length of the SQL statement.
    if (rm_slv_batch_size > 400)
    {
        rm_slv_batch_size = 400;
    }

    wrk_max_len = max_size + ((max_size * rm_slv_percent_exp) / 100) + rm_slv_fixed_exp;

    if (rm_slv_max_len < wrk_max_len)
    {
        rm_slv_batch_size = 1;          // If we exceeded the size then process the batch one at a time. 
    }

    // Make sure we limit the batch size so that we don't use significantly more memory than the maximum configured.
    int64_t temp_cnt = rm_slv_meg * rm_slv_max_mem_m / wrk_max_len;
    if (temp_cnt < rm_slv_batch_size)
    {
        rm_slv_batch_size = (int)temp_cnt;
    }

    if (calc_sizes.size() < rm_slv_batch_size)
    {
        rm_slv_batch_size = calc_sizes.size();
    }

    if (rm_slv_batch_size < 1)
    {
        rm_slv_batch_size = 1;
    }

    cor_puids = (EIM_uid_t*)SM_alloc(sizeof(EIM_uid_t) * (rm_slv_batch_size + 1));
    cor_lens = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));

    logical trans_was_active = true;
    EIM_select_var_t vars[4];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    str_len_val_status(true, "Start ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

    while (wrk_start_pos < puids.size())
    {
        trans_was_active = true;

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

        int cor_offset = 0;

        // Calculate the batch size for this sql statement by
        wrk_batch_size = puids.size() - wrk_start_pos;

        if (wrk_batch_size > rm_slv_batch_size)
        {
            wrk_batch_size = rm_slv_batch_size;
        }

        if (wrk_batch_size < 1)
        {
            if (wrk_start_pos < puids.size())
            {
                wrk_batch_size = 1;
            }
            else
            {
                wrk_batch_size = 0;
            }
        }

        if (wrk_batch_size > 0)
        {
            // Build SQL to retrieve actual data from the database
            std::stringstream sql;

            sql << "SELECT a.puid puid, a." << cls_col << " data";
            sql << " FROM " << cls_tbl << " a WHERE a.puid IN (";

            for (int i = 0; i < wrk_batch_size; i++)
            {
                if (i > 0)
                {
                    sql << ",";
                }

                sql << "'" << puids[wrk_start_pos++] << "'";
            }

            sql << ")";

            EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
            EIM_select_col(&(vars[1]), EIM_varchar, "data", wrk_max_len + 1, false);

            ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 2, vars, 0, NULL);
            EIM_check_error("Retrieving string data\n");

            if (report != NULL)
            {
                for (row = report; row != NULL; row = row->next)
                {
                    rpt_worked_cnt++;

                    char* tmp_puid = NULL;
                    EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);

                    if (tmp_puid == NULL)
                    {
                        rpt_unknown_cnt++;
                        continue;
                    }

                    char* tmp_data = NULL;
                    int str_len = 0;
                    logical correction_required = false;

                    EIM_find_value(headers, row->line, "data", EIM_varchar, &tmp_data);

                    if (tmp_data != NULL)
                    {
                        str_len = strlen(tmp_data);

                        if (max_size < str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_under_cnt++;
                        }
                        else if (max_size > str_len)
                        {
                            correction_required = false;
                            rpt_over_cnt++;
                        }
                        else
                        {
                            correction_required = false;
                            rpt_success_cnt++;
                        }
                    }
                    else
                    {
                        correction_required = false;
                        rpt_unknown_cnt++;
                    }

                    if (correction_required)
                    {
                        // resolves coverity defect 506006 : String Overflow
                        if (sizeof(cor_puids[cor_offset]) > strlen(tmp_puid))
                        {
                            strcpy(cor_puids[cor_offset], tmp_puid);
                        }
                        else
                        {
                            ERROR_internal(ERROR_line, "Size not enough to copy '%s' ", tmp_puid);
                        }

                        cor_lens[cor_offset] = str_len;
                        cor_offset++;
                    }
                }
                EIM_free_result(headers, report);
                report = NULL;
                headers = NULL;
            }

            if (cor_offset > 0 && DDS_is_pom_parameter_enabled("RM_STR_LEN_LOG_BAD_UIDS", true))
            {
                int counter = 0;

                do
                {
                    {
                        std::stringstream msg;
                        msg << "\nreference_manager -str_len_val -u=user -p=password -g=dba -from=" << cls << ":" << attr << " -commit";

                        for (int i = 0; i < 20 && counter < cor_offset; i++, counter++)
                        {
                            msg << " -uid=" << (const char*)(cor_puids[counter]);
                        }
                        msg << "\n";

                        logger()->printf(msg.str().c_str());
                    }
                } while (counter < cor_offset);
            }

            if (cor_offset > 0 && args->commit_flag)
            {
                // Truncate strings ... remove a few characters... most of the time one (1) character. 
                ifail = truncate_strings(cls_tbl, cls_col, max_size, cor_puids, cor_lens, cor_offset);
                rpt_corrected_cnt += cor_offset;
            }
        }

        str_len_val_status(false, NULL, func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

        if (!trans_was_active)
        {
            EIM_commit_transaction("validate_long_string_sizes()");
        }
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    *bad_count += rpt_eligible_cnt;
    str_len_val_status(true, "Final ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);
    return(ifail);
}

/*------------------------------------------------------------------------
** Truncate strings to fit within max size.
** ----------------------------------------------------------------------- */
static int truncate_strings(const char *tbl, const char *col, int max_size, EIM_uid_t *uids, int* ints, int size)
{
    int ifail = OK;

    if (size < 1)
    {
        return(ifail);
    }

    const int maxUpdateSize = EIM_get_max_insert_size();
    int*        adj = (int*)SM_alloc(sizeof(int) * (size + 1));
    int         batch_size = 0;
    EIM_uid_t*  uid_ptr = uids;
    int*        int_ptr = adj;

    if (maxUpdateSize < size)
    {
        batch_size = maxUpdateSize;
        if (batch_size < 1)
            batch_size = EIM_ARRAY_MAX_SIZE;
    }
    else
    {
        batch_size = size;
    }

    // Calculate the number of characters we want to truncate. 
    {
        int  rm_slv_max_bytes_per_char = 4;             // UTF-8 we use 4. The divisor value for calculation of additional characters to be removed. 
        rm_slv_max_bytes_per_char = DDS_ask_pom_parameter_int("rm_slv_max_bytes_per_char", rm_slv_max_bytes_per_char);
        int adjval = 0;

        for (int i = 0; i < size; i++)
        {
            if (ints[i] <= max_size)
            {
                adj[i] = 0;
                continue;
            }

            adj[i] = 1;

            if (rm_slv_max_bytes_per_char > 0)
            {
                adjval = ints[i] - max_size;
                adjval--;

                adjval = adjval / rm_slv_max_bytes_per_char;
                adj[i] = adjval + 1;
            }
        }
    }

    int i = 0;
    int this_batch = size;
    this_batch = this_batch % batch_size;

    if (this_batch == 0)
    {
        this_batch = batch_size;
    }

    while (i < size)
    {

        for (int j = 0; j < this_batch; j++)
        {
            char* sql = NULL;

            sql = SM_sprintf( "UPDATE %s SET %s = %s(%s,1,(%s(%s) - %d)) WHERE puid = '%s'", tbl, col, db_substr_fun(), col, db_char_len_fun(), col, *( int_ptr + i + j ), ( uid_ptr + i + j ) );
    
            // Do update
            ifail = EIM_exec_imm_array_bind(sql, "truncate_strings(): truncating strings", 1, 0, NULL);
            EIM_check_error("truncate_strings(): truncating strings");

            SM_free(sql);
        }

        i = i + this_batch;
        this_batch = batch_size;
    }

    if (i != size)
    {
        ERROR_raise(ERROR_line, POM_internal_error, "validate_long_string_sizes(): Not all data written to %s table, missing at least %d records. (%d)", tbl, size - i, ifail);
    }

    SM_free(adj);
    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted sizes for VLA POM_string attributes.
** ----------------------------------------------------------------------- */
static int get_vla_string_sizes(const char *att_tbl, const char *att_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes)
{
    int ifail = OK;

    if ( EIM_dbplat() != EIM_dbplat_mssql && EIM_dbplat() != EIM_dbplat_oracle )
    {
        ERROR_raise(ERROR_line, POM_internal_error, "Functionality is not supported for RDBMSs other than MS SQL Server and Oracle.");
    }

    int  rm_slv_max_bytes_per_char = 4;     // UTF-8 we use 4. Used to calculate minimum length before we consider it a possible problem. 
    rm_slv_max_bytes_per_char = DDS_ask_pom_parameter_int("rm_slv_max_bytes_per_char", rm_slv_max_bytes_per_char);

    int storge_bytes_per_ascii_char = 2;    // For unicode there are two bytes for every ascii character

    if (!EIM_unicode_enabled())
    {
        storge_bytes_per_ascii_char = 1;    // If not unicode we assume UTF-8, in which case we have 1 storage byte per ascii character
    }

    puids.clear();
    seqs.clear();
    calc_sizes.clear();

    logical trans_was_active = true;
    EIM_select_var_t vars[3];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

    std::stringstream sql;
    logical where_added = false;

    sql << "SELECT a.puid puid, a.pseq pseq, " << db_byte_len_fun() << "(a." << att_col << ") calc_size";
    sql << " FROM " << att_tbl << " a";

    if ( rm_slv_max_bytes_per_char > 0 )
    {
        sql << " WHERE " << db_byte_len_fun() << "(a." << att_col << ") >= " << (int)( ( max_size * storge_bytes_per_ascii_char ) / rm_slv_max_bytes_per_char );
        where_added = true;
    }

    if (uid != NULL || (uid_vec != NULL && uid_vec->size() > 0))
    {
        if (!where_added)
        {
            sql << " WHERE a.puid IN (";
            where_added = true;
        }
        else
        {
            sql << " AND a.puid IN (";
        }

        int inc_cnt = 0;

        if (uid != NULL)
        {
            sql << "'" << uid << "'";
            inc_cnt++;
        }

        if (uid_vec != NULL && uid_vec->size() > 0)
        {
            for (int i = 0; i < uid_vec->size(); i++)
            {
                if (inc_cnt > 0)
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec)[i] << "'";
                inc_cnt++;
            }
        }
        sql << ")";
    }

    sql << " ORDER BY puid, pseq, calc_size ASC";

    EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
    EIM_select_col(&(vars[1]), EIM_integer, "pseq", sizeof(int), false);
    EIM_select_col(&(vars[2]), EIM_integer, "calc_size", sizeof(int), false);

    ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 3, vars, 0, NULL);
    EIM_check_error("Retrieving string sizes\n");

    if (report != NULL)
    {
        for (row = report; row != NULL; row = row->next)
        {
            char* tmp_puid = NULL;
            EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);
            puids.push_back(tmp_puid);

            int* tmp_seq = NULL;
            EIM_find_value(headers, row->line, "pseq", EIM_integer, &tmp_seq);
            seqs.push_back(*tmp_seq);

            int* tmp_size = NULL;
            EIM_find_value(headers, row->line, "calc_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                calc_sizes.push_back(*tmp_size);
            }
            else
            {
                calc_sizes.push_back(0);
            }
        }
    }
    EIM_free_result(headers, report);
    report = NULL;
    headers = NULL;

    if (!trans_was_active)
    {
        EIM_commit_transaction("get_vla_string_sizes()");
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return(ifail);
}

/*------------------------------------------------------------------------
** Validate the size of data in a VLA POM_string attribute against
** the maximum size of the field.
** We'll assume that if the stored data length is less than 1/4 of the max data length
** that it will happily fit within the max buffer size when read from the database.
** ----------------------------------------------------------------------- */
static int validate_vla_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = OK;
    // RM_SLV_ stands for Reference Manager String Length Validation
    int rm_slv_batch_size = 65500;       // Number of records processed per batch
    int rm_slv_max_len = 100000;         // Maximum length of records within the batch, otherwise one at a time.
    int rm_slv_percent_exp = 30;         // Percentage of expected expansion due to none single byte characters
    int rm_slv_fixed_exp = 100;          // Additional fixed byte expansion to provide additional memory for unforseen expansions...
    int rm_slv_max_mem_m = 6000;         // Maximum memory to be used within each batch... unit single record processing kicks in.
    int64_t rm_slv_meg = 1024 * 1024;       // Megabyte

    int rpt_worked_cnt = 0;              // Number of records that were processed
    int rpt_success_cnt = 0;             // Number of records where data length matches the stored length
    int rpt_under_cnt = 0;               // Number of records where the stored count under represents the data length
    int rpt_over_cnt = 0;                // Number of records where the stored count over represents the data length
    int rpt_unknown_cnt = 0;             // Number of records that we don't know about (NULL for size returned etc...)
    int rpt_eligible_cnt = 0;            // Number of records that are eligible for correction
    int rpt_corrected_cnt = 0;           // Number of records where the stored count was actually corrected

    int wrk_start_pos = 0;               // UID to start with.
    int wrk_max_len = 0;                 // Maximum size of a buffer that is expected to retrieve data
    int wrk_batch_size = 0;              // Batch size for the current batch. 

                                         // Check configuration values. 
    rm_slv_batch_size = DDS_ask_pom_parameter_int("rm_slv_batch_size", rm_slv_batch_size);
    rm_slv_max_len = DDS_ask_pom_parameter_int("rm_slv_max_len", rm_slv_max_len);
    rm_slv_percent_exp = DDS_ask_pom_parameter_int("rm_slv_percent_exp", rm_slv_percent_exp);
    rm_slv_fixed_exp = DDS_ask_pom_parameter_int("rm_slv_fixed_exp", rm_slv_fixed_exp);
    rm_slv_max_mem_m = DDS_ask_pom_parameter_int("rm_slv_max_mem_m", rm_slv_max_mem_m);
    EIM_uid_t* cor_puids = NULL;
    int*       cor_lens = NULL;
    int*       cor_seqs = NULL;

    // Filter to ensure that we output bad uids only once into the syslog. 
    std::map< std::string, int > dups;
    std::map< std::string, int >::iterator it;

    // As we are currently not using ENQ or temporary tables we will limit the batch size to 400 to limit the length of the SQL statement.
    if (rm_slv_batch_size > 400)
    {
        rm_slv_batch_size = 400;
    }

    wrk_max_len = max_size + ((max_size * rm_slv_percent_exp) / 100) + rm_slv_fixed_exp;

    if (rm_slv_max_len < wrk_max_len)
    {
        rm_slv_batch_size = 1;
    }

    // Make sure we limit the batch size so that we don't use significantly more memory than the maximum configured.
    int64_t temp_cnt = rm_slv_meg * rm_slv_max_mem_m / wrk_max_len;

    if (temp_cnt < rm_slv_batch_size)
    {
        rm_slv_batch_size = (int)temp_cnt;
    }

    if (calc_sizes.size() < rm_slv_batch_size)
    {
        rm_slv_batch_size = calc_sizes.size();
    }

    if (rm_slv_batch_size < 1)
    {
        rm_slv_batch_size = 1;
    }

    cor_puids = (EIM_uid_t*)SM_alloc(sizeof(EIM_uid_t) * (rm_slv_batch_size + 1));
    cor_seqs = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));
    cor_lens = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));

    logical trans_was_active = true;
    EIM_select_var_t vars[3];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    str_len_val_status(true, "Start ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

    while (wrk_start_pos < puids.size())
    {
        trans_was_active = true;

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

        int cor_offset = 0;

        // Calculate the batch size for this sql statement by
        wrk_batch_size = puids.size() - wrk_start_pos;

        if (wrk_batch_size > rm_slv_batch_size)
        {
            wrk_batch_size = rm_slv_batch_size;
        }

        if (wrk_batch_size < 1)
        {
            if (wrk_start_pos < puids.size())
            {
                wrk_batch_size = 1;
            }
            else
            {
                wrk_batch_size = 0;
            }
        }

        if (wrk_batch_size > 0)
        {
            // Build SQL to retrieve actual data from the database
            std::stringstream sql;

            sql << "SELECT a.puid puid, a.pseq pseq, a." << cls_col << " data";
            sql << " FROM " << cls_tbl << " a WHERE";

            for (int i = 0; i < wrk_batch_size; i++)
            {
                if (i > 0)
                {
                    sql << " OR";
                }

                sql << " (a.puid = '" << puids[wrk_start_pos] << "' AND a.pseq = " << seqs[wrk_start_pos] << ")";
                wrk_start_pos++;
            }

            EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
            EIM_select_col(&(vars[1]), EIM_integer, "pseq", sizeof(int), false);
            EIM_select_col(&(vars[2]), EIM_varchar, "data", wrk_max_len + 1, false);

            ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 3, vars, 0, NULL);
            EIM_check_error("Retrieving string data\n");

            if (report != NULL)
            {
                for (row = report; row != NULL; row = row->next)
                {
                    rpt_worked_cnt++;

                    char* tmp_puid = NULL;
                    EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);

                    if (tmp_puid == NULL)
                    {
                        rpt_unknown_cnt++;
                        continue;
                    }

                    int* tmp_seq = NULL;
                    EIM_find_value(headers, row->line, "pseq", EIM_integer, &tmp_seq);

                    if (tmp_seq == NULL)
                    {
                        rpt_unknown_cnt++;
                        continue;
                    }

                    if (tmp_seq == NULL)
                    {
                        rpt_unknown_cnt++;
                        continue;
                    }

                    char* tmp_data = NULL;
                    int str_len = 0;
                    logical correction_required = false;

                    EIM_find_value(headers, row->line, "data", EIM_varchar, &tmp_data);

                    if (tmp_data != NULL)
                    {
                        str_len = strlen(tmp_data);

                        if (max_size < str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_under_cnt++;
                        }
                        else if (max_size > str_len)
                        {
                            correction_required = false;
                            rpt_over_cnt++;
                        }
                        else
                        {
                            correction_required = false;
                            rpt_success_cnt++;
                        }
                    }
                    else
                    {
                        correction_required = false;
                        rpt_unknown_cnt++;
                    }

                    if (correction_required)
                    {
                        // resolves coverity defect 506014 : String Overflow
                        if (sizeof(cor_puids[cor_offset]) > strlen(tmp_puid))
                        {
                            strcpy(cor_puids[cor_offset], tmp_puid);
                        }
                        else
                        {
                            ERROR_internal(ERROR_line, "Size not enough to copy '%s' ", tmp_puid);
                        }

                        cor_seqs[cor_offset] = *tmp_seq;
                        cor_lens[cor_offset] = str_len;
                        cor_offset++;
                    }
                }
                EIM_free_result(headers, report);
                report = NULL;
                headers = NULL;
            }

            if (cor_offset > 0 && DDS_is_pom_parameter_enabled("RM_STR_LEN_LOG_BAD_UIDS", true))
            {
                int counter = 0;

                do
                {
                    logical valid_uids = false;
                    {
                        std::stringstream msg;
                        msg << "\nreference_manager -str_len_val -u=user -p=password -g=dba -from=" << cls << ":" << attr << " -commit";

                        for (int i = 0; i < 20 && counter < cor_offset; i++, counter++)
                        {
                            std::string temp_uid = cor_puids[counter];
                            it = dups.find(temp_uid);

                            if (it == dups.end())
                            {
                                valid_uids = true;
                                dups.insert(std::pair< std::string, int>(temp_uid, 0));
                                msg << " -uid=" << (const char*)(cor_puids[counter]);
                            }
                        }
                        msg << "\n";

                        if (valid_uids)
                        {
                            valid_uids = false;
                            logger()->printf(msg.str().c_str());
                        }
                    }
                } while (counter < cor_offset);
            }

            if (cor_offset > 0 && args->commit_flag)
            {
                // Truncate strings ... remove a few characters... most of the time one (1) character. 
                ifail = truncate_vla_strings(cls_tbl, cls_col, max_size, cor_puids, cor_seqs, cor_lens, cor_offset);
                rpt_corrected_cnt += cor_offset;
            }
        }

        str_len_val_status(false, NULL, func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

        if (!trans_was_active)
        {
            EIM_commit_transaction("validate_vla_string_sizes()");
        }
    }

    ERROR_RECOVER

        const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    *bad_count += rpt_eligible_cnt;
    str_len_val_status(true, "Final ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);
    return(ifail);
}

/*------------------------------------------------------------------------
** Truncate vla strings to fit within max size.
** ----------------------------------------------------------------------- */
static int truncate_vla_strings(const char *tbl, const char *col, int max_size, EIM_uid_t *uids, int* seqs, int* ints, int size)
{
    int ifail = OK;

    if (size < 1)
    {
        return(ifail);
    }

    int*        adj = (int*)SM_alloc(sizeof(int) * (size + 1));
    EIM_uid_t*  uid_ptr = uids;
    int*        int_ptr = adj;
    int*        seq_ptr = seqs;

    // Calculate the number of characters we want to truncate. 
    {
        int  rm_slv_max_bytes_per_char = 4;             // The divisor value for calculation of additional characters to be removed. 
        rm_slv_max_bytes_per_char = DDS_ask_pom_parameter_int("rm_slv_max_bytes_per_char", rm_slv_max_bytes_per_char);
        int adjval = 0;

        for (int i = 0; i < size; i++)
        {
            if (ints[i] <= max_size)
            {
                adj[i] = 0;
                continue;
            }

            adj[i] = 1;

            if (rm_slv_max_bytes_per_char > 0)
            {
                adjval = ints[i] - max_size;
                adjval--;

                adjval = adjval / rm_slv_max_bytes_per_char;
                adj[i] = adjval + 1;
            }
        }
    }

    for (int i = 0; i < size; i++)
    {
        char* sql = SM_sprintf( "UPDATE %s SET %s = %s(%s,1,(%s(%s) - %d)) WHERE puid = '%s' AND pseq = %d", tbl, col, db_substr_fun(), col, db_char_len_fun(), col, *( int_ptr + i ), ( uid_ptr + i ), *( seq_ptr + i ) );

        // Do update
        ifail = EIM_exec_imm_array_bind(sql, "truncate_strings(): truncating vla strings", 1, 0, NULL);
        EIM_check_error("truncate_strings(): truncating vla strings");

        SM_free(sql);
    }

    SM_free(adj);

    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted sizes for LA (large array) POM_string attributes.
** ----------------------------------------------------------------------- */
static int get_la_string_sizes(const char *att_tbl, const char *att_col, int max_size, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes)
{
    int ifail = get_vla_string_sizes(att_tbl, att_col, max_size, uid, uid_vec, puids, seqs, calc_sizes);
    return(ifail);
}

/*------------------------------------------------------------------------
** Validate the size of data in a LA (large array) POM_string attribute against
** the maximum size of the field.
** We'll assume that if the stored data length is less than 1/4 of the max data length
** that it will happily fit within the max buffer size when read from the database.
** ----------------------------------------------------------------------- */
static int validate_la_string_sizes(const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, int max_size, std::vector< std::string > &puids, std::vector< int > &seqs, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = validate_vla_string_sizes("Str(LA)", cls, attr, cls_tbl, cls_col, max_size, puids, seqs, calc_sizes, bad_count);
    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted and stored sizes for long string data.
** ----------------------------------------------------------------------- */
static int get_long_string_sizes(const char *cls_tbl, const char *cls_col, const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes)
{
    int ifail = OK;
    // onst char *potname = DDS_ask_tname((DDS_class_p_t) OM_ask_class_of_class_id (OM_lookup_class("POM_object")));
    if ( EIM_dbplat() != EIM_dbplat_mssql && EIM_dbplat() != EIM_dbplat_oracle )
    {
        ERROR_raise(ERROR_line, POM_internal_error, "Functionality is not supported for RDBMSs other than MS SQL Server and Oracle.");
    }

    puids.clear();
    calc_sizes.clear();
    stored_sizes.clear();

    logical trans_was_active = true;
    EIM_select_var_t vars[3];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    if (!EIM_is_transaction_active())
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    std::stringstream sql;

    sql << "SELECT a.puid puid, " << db_byte_len_fun() << "(a." << att_col << ") calc_size, b." << cls_col << " stored_size";
    sql << " FROM " << att_tbl << " a, " << cls_tbl << " b WHERE a.puid = b.puid";  

    if (uid != NULL || (uid_vec != NULL && uid_vec->size() > 0))
    {
        sql << " AND a.puid IN (";
        int inc_cnt = 0;

        if (uid != NULL)
        {
            sql << "'" << uid << "'";
            inc_cnt++;
        }

        if (uid_vec != NULL && uid_vec->size() > 0)
        {
            for (int i = 0; i < uid_vec->size(); i++)
            {
                if (inc_cnt > 0)
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec)[i] << "'";
                inc_cnt++;
            }
        }
        sql << ")";
    }

    sql << " ORDER BY calc_size ASC";

    EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
    EIM_select_col(&(vars[1]), EIM_integer, "calc_size", sizeof(int), false);
    EIM_select_col(&(vars[2]), EIM_integer, "stored_size", sizeof(int), false);

    ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 3, vars, 0, NULL);
    EIM_check_error("Retrieving long-string sizes\n");

    if (report != NULL)
    {
        for (row = report; row != NULL; row = row->next)
        {
            char* tmp_puid = NULL;
            EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);
            puids.push_back(tmp_puid);

            int* tmp_size = NULL;
            EIM_find_value(headers, row->line, "calc_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                calc_sizes.push_back(*tmp_size);
            }
            else
            {
                calc_sizes.push_back(0);
            }

            tmp_size = NULL;
            EIM_find_value(headers, row->line, "stored_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                stored_sizes.push_back(*tmp_size);
            }
            else
            {
                stored_sizes.push_back(0);
            }
        }
        EIM_free_result(headers, report);
        report = NULL;
        headers = NULL;
    }

    if (!trans_was_active)
    {
        EIM_commit_transaction("get_long_string_sizes()");
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return(ifail);
}

/*------------------------------------------------------------------------
** Validate that the actual string sizes match the stored string sizes
** and optionally correct the stored string sizes.
** ----------------------------------------------------------------------- */
static int validate_long_string_sizes(const char* func, const char* cls, const char* attr, const char *cls_tbl, const char *cls_col, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = OK;
    // RM_SLV_ stands for Reference Manager String Length Validation
    int rm_slv_batch_size = 65500;       // Number of records processed per batch
    int rm_slv_max_len = 200000;         // Maximum length of records within the batch
    int rm_slv_percent_exp = 30;         // Percentage of expected expansion due to none single byte characters
    int rm_slv_fixed_exp = 100;          // Additional fixed byte expansion to provide additional memory for unforseen expansions...
    int rm_slv_max_mem_m = 6000;         // Maximum memory to be used within each batch... unit single record processing kicks in.
    long rm_slv_max_int = 2147483647;    // Maximum integer value
    int64_t rm_slv_meg = 1024 * 1024;       // Megabyte

    int rpt_worked_cnt = 0;              // Number of records that were processed
    int rpt_success_cnt = 0;             // Number of records where data length matches the stored length
    int rpt_under_cnt = 0;               // Number of records where the stored count under represents the data length
    int rpt_over_cnt = 0;                // Number of records where the stored count over represents the data length
    int rpt_unknown_cnt = 0;             // Number of records that we don't know about (NULL for size returned etc...)
    int rpt_eligible_cnt = 0;            // Number of records that are eligible for correction
    int rpt_corrected_cnt = 0;           // Number of records where the stored count was actually corrected

    int wrk_start_pos = 0;               // UID to start with.
    int wrk_batch_size = 0;              // Batch size for the current SQL statement
    int wrk_max_len = 0;                 // Max data length for this SQL statement on this platform
    int wrk_max_calc = 0;                // The maximum caculated size for this batch

                                         // Check configuration values. 
    rm_slv_batch_size  = DDS_ask_pom_parameter_int("rm_slv_batch_size",  rm_slv_batch_size);
    rm_slv_max_len     = DDS_ask_pom_parameter_int("rm_slv_max_len",     rm_slv_max_len);
    rm_slv_percent_exp = DDS_ask_pom_parameter_int("rm_slv_percent_exp", rm_slv_percent_exp);
    rm_slv_fixed_exp   = DDS_ask_pom_parameter_int("rm_slv_fixed_exp",   rm_slv_fixed_exp);
    rm_slv_max_mem_m   = DDS_ask_pom_parameter_int("rm_slv_max_mem_m",   rm_slv_max_mem_m);
    EIM_uid_t* cor_puids = NULL;
    int*       cor_lens = NULL;

    int storge_bytes_per_ascii_char = 2;    // For unicode there are two bytes for every ascii character

    if (!EIM_unicode_enabled())
    {
        storge_bytes_per_ascii_char = 1;    // If not unicode we assume UTF-8, in which case we have 1 storage byte per ascii character
    }

    // As we are currently not using ENQ or temporary tables we will limit the batch size to 200 to limit the length of the SQL statement.
    if (rm_slv_batch_size > 200)
    {
        rm_slv_batch_size = 200;
    }

    // Make sure we limit the batch size so that we don't use significantly more memory than the maximum configured.
    int64_t temp_cnt = rm_slv_meg * rm_slv_max_mem_m / rm_slv_max_len;
    if (temp_cnt < rm_slv_batch_size)
    {
        rm_slv_batch_size = (int)temp_cnt;
    }

    if (calc_sizes.size() < rm_slv_batch_size)
    {
        rm_slv_batch_size = calc_sizes.size();
    }

    if (rm_slv_batch_size < 1)
    {
        rm_slv_batch_size = 1;
    }

    cor_puids = (EIM_uid_t*)SM_alloc(sizeof(EIM_uid_t) * (rm_slv_batch_size + 1));
    cor_lens = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));

    logical trans_was_active = true;
    EIM_select_var_t vars[4];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    str_len_val_status(true, "Start ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

    while (wrk_start_pos < puids.size())
    {
        trans_was_active = true;

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

        int cor_offset = 0;

        // Any time a record is larger that rm_slv_max_len, we want to set the batch size to one. 
        // So look through the next batch of values and check for the first large data value. 

        wrk_batch_size = 1;
        wrk_max_calc = 0;

        if (rm_slv_batch_size > 1 && wrk_start_pos < puids.size())
        {
            int term_pos = wrk_start_pos;

            for (int j = 0; j < rm_slv_batch_size && term_pos < puids.size(); j++, term_pos++)
            {
                if (calc_sizes[wrk_start_pos + j] / storge_bytes_per_ascii_char > rm_slv_max_len)
                {
                    break;
                }

                if (calc_sizes[wrk_start_pos + j] > wrk_max_calc)
                {
                    wrk_max_calc = calc_sizes[wrk_start_pos + j];
                }
            }

            if (term_pos - wrk_start_pos > 1)
            {
                wrk_batch_size = term_pos - wrk_start_pos;
            }
        }

        if (wrk_batch_size == 1)
        {
            rm_slv_batch_size = 1;
        }

        if (wrk_batch_size > 0)
        {
            // Calculate max buffer size based on configured expected expansion ratio.
            // Start with the largest cacluate size in our batch.
            long tmp_max_len = wrk_max_calc;

            if (wrk_batch_size == 1)
            {
                // If the batch size is just one the use the calcualted from for this record.
                tmp_max_len = calc_sizes[wrk_start_pos];
            }

            if (EIM_unicode_enabled())
            {
                tmp_max_len = tmp_max_len >> 1;
                tmp_max_len++;
            }

            tmp_max_len = tmp_max_len + (long)(tmp_max_len * rm_slv_percent_exp / 100) + rm_slv_fixed_exp;

            if (tmp_max_len > rm_slv_max_int)
            {
                wrk_max_len = (int)rm_slv_max_int;
            }
            else
            {
                wrk_max_len = (int)tmp_max_len;
            }

            // Build SQL to retrieve actual data from the database
            std::stringstream sql;

            sql << "SELECT a.puid puid, a." << att_col << " data, b." << cls_col << " stored_size";
            sql << " FROM " << att_tbl << " a, " << cls_tbl << " b WHERE a.puid = b.puid AND a.puid IN (";

            for (int i = 0; i < wrk_batch_size; i++)
            {
                if (i > 0)
                {
                    sql << ",";
                }

                sql << "'" << puids[wrk_start_pos++] << "'";
            }

            sql << ")";

            EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
            EIM_select_col(&(vars[1]), EIM_varchar, "data", wrk_max_len, false);
            EIM_select_col(&(vars[2]), EIM_integer, "stored_size", sizeof(int), false);

            ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 3, vars, 0, NULL);
            EIM_check_error("Retrieving long-string data\n");

            if (report != NULL)
            {
                for (row = report; row != NULL; row = row->next)
                {
                    rpt_worked_cnt++;

                    char* tmp_puid = NULL;
                    EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);

                    char* tmp_data = NULL;
                    int str_len = 0;
                    logical correction_required = false;

                    EIM_find_value(headers, row->line, "data", EIM_varchar, &tmp_data);

                    if (tmp_data != NULL)
                    {
                        str_len = strlen(tmp_data);
                    }

                    int *tmp_size = NULL;
                    EIM_find_value(headers, row->line, "stored_size", EIM_integer, &tmp_size);

                    if (tmp_size != NULL)
                    {
                        if (*tmp_size < str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_under_cnt++;
                        }
                        else if (*tmp_size > str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_over_cnt++;
                        }
                        else
                        {
                            correction_required = false;
                            rpt_success_cnt++;
                        }
                    }
                    else
                    {
                        correction_required = true;
                        rpt_eligible_cnt++;
                        rpt_unknown_cnt++;
                    }

                    if (correction_required)
                    {
                        //  resolves coverity defect 506025 : String Overflow
                        if (sizeof(cor_puids[cor_offset]) > strlen(tmp_puid))
                        {
                            strcpy(cor_puids[cor_offset], tmp_puid);
                        }
                        else
                        {
                            ERROR_internal(ERROR_line, "Size not enough to copy '%s' ", tmp_puid);
                        }

                        cor_lens[cor_offset] = str_len;
                        cor_offset++;
                    }
                }
                EIM_free_result(headers, report);
                report = NULL;
                headers = NULL;
            }

            if (cor_offset > 0 && DDS_is_pom_parameter_enabled("RM_STR_LEN_LOG_BAD_UIDS", true))
            {
                int counter = 0;

                do
                {
                    {
                        std::stringstream msg;
                        msg << "\nreference_manager -str_len_val -u=user -p=password -g=dba -from=" << cls << ":" << attr << " -commit";

                        for (int i = 0; i < 20 && counter < cor_offset; i++, counter++)
                        {
                            msg << " -uid=" << (const char*)(cor_puids[counter]);
                        }
                        msg << "\n";

                        logger()->printf(msg.str().c_str());
                    }
                } while (counter < cor_offset);
            }

            if (cor_offset > 0 && args->commit_flag)
            {
                // 
                // Update data lengths here. 
                //
                ifail = bulk_update_integers(cls_tbl, cls_col, cor_puids, cor_lens, cor_offset);
                rpt_corrected_cnt += cor_offset;
            }
        }

        str_len_val_status(false, NULL, func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

        if (!trans_was_active)
        {
            EIM_commit_transaction("validate_long_string_sizes()");
        }
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    SM_free(cor_puids);
    SM_free(cor_lens);

    *bad_count = rpt_eligible_cnt;
    str_len_val_status(true, "Final ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);
    return(ifail);
}

/*------------------------------------------------------------------------
** Bulk update integers for specified UIDs.
** ----------------------------------------------------------------------- */
static int bulk_update_integers(const char *tbl, const char *col, EIM_uid_t *uids, int* ints, int size)
{
    int ifail = OK;

    if (size < 1)
    {
        return(ifail);
    }

    const int maxUpdateSize = EIM_get_max_insert_size();
    int         batch_size = 0;
    EIM_uid_t*  uid_ptr = uids;
    int*        int_ptr = ints;

    if (maxUpdateSize < size)
    {
        batch_size = maxUpdateSize;
        if (batch_size < 1)
            batch_size = EIM_ARRAY_MAX_SIZE;
    }
    else
    {
        batch_size = size;
    }

    // Allocate an array of pointers to bind array value pointers.
    EIM_bind_array_value_p_t bvs[2];
    bvs[0] = static_cast<EIM_bind_array_value_p_t>(SM_alloc(sizeof(EIM_bind_array_value_t)));
    bvs[1] = static_cast<EIM_bind_array_value_p_t>(SM_alloc(sizeof(EIM_bind_array_value_t)));

    bvs[0]->type = EIM_integer;
    bvs[0]->len = sizeof(int);
    bvs[0]->array_size = 0;
    bvs[0]->ind = (short *)SM_calloc(batch_size, sizeof(short));
    bvs[0]->value = NULL;

    bvs[1]->type = EIM_puid;
    bvs[1]->len = sizeof(EIM_uid_t);
    bvs[1]->array_size = 0;
    bvs[1]->ind = (short *)SM_calloc(batch_size, sizeof(short));
    bvs[1]->value = NULL;

    int i = 0;
    int this_batch = size;
    this_batch = this_batch % batch_size;

    if (this_batch == 0)
    {
        this_batch = batch_size;
    }

    while (i < size)
    {
        bvs[0]->array_size = this_batch;
        bvs[0]->value = int_ptr + i;

        bvs[1]->array_size = this_batch;
        bvs[1]->value = uid_ptr + i;

        char* sql = SM_sprintf("UPDATE %s SET %s = :1 WHERE puid = :2", tbl, col);

        // Do update
        ifail = EIM_exec_imm_array_bind(sql, "bulk_update_integers(): bulk updating string lengths", this_batch, 2, bvs);
        EIM_check_error("bulk_update_integers(): bulk updating string lengths");

        SM_free(sql);

        i = i + this_batch;
        this_batch = batch_size;

        bvs[0]->array_size = 0;
        bvs[0]->value = NULL;

        bvs[1]->array_size = 0;
        bvs[1]->value = NULL;
    }

    if (i != size)
    {
        ERROR_raise(ERROR_line, POM_internal_error, "validate_long_string_sizes(): Not all data written to %s table, missing at least %d records. (%d)", tbl, size - i, ifail);
    }

    SM_free(bvs[0]->ind);
    SM_free(bvs[1]->ind);
    SM_free(bvs[0]);
    SM_free(bvs[1]);

    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted and stored sizes for long string data.
** Data is returned in ascending order by calculated size.
** ----------------------------------------------------------------------- */
static int get_vla_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes)
{
    int ifail = OK;

    if ( EIM_dbplat() != EIM_dbplat_mssql && EIM_dbplat() != EIM_dbplat_oracle )
    {
        ERROR_raise(ERROR_line, POM_internal_error, "Functionality is not supported for RDBMSs other than MS SQL Server and Oracle.");
    }

    puids.clear();
    pseqs.clear();
    calc_sizes.clear();
    stored_sizes.clear();

    logical trans_was_active = true;
    EIM_select_var_t vars[4];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

    std::stringstream sql;

    sql << "SELECT a.puid puid, a.pseq pseq, a.pvall stored_size, " << db_byte_len_fun() << "(a." << att_col << ") calc_size FROM " << att_tbl << " a";

    if (uid != NULL || (uid_vec != NULL && uid_vec->size() > 0))
    {
        sql << " WHERE a.puid IN (";

        int inc_cnt = 0;

        if (uid != NULL)
        {
            sql << "'" << uid << "'";
            inc_cnt++;
        }

        if (uid_vec != NULL && uid_vec->size() > 0)
        {
            for (int i = 0; i < uid_vec->size(); i++)
            {
                if (inc_cnt > 0)
                {
                    sql << ",";
                }

                sql << "'" << (*uid_vec)[i] << "'";
                inc_cnt++;
            }
        }
        sql << ")";
    }

    sql << " ORDER BY calc_size ASC";

    EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
    EIM_select_col(&(vars[1]), EIM_integer, "pseq", sizeof(int), false);
    EIM_select_col(&(vars[2]), EIM_integer, "stored_size", sizeof(int), false);
    EIM_select_col(&(vars[3]), EIM_integer, "calc_size", sizeof(int), false);

    ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 4, vars, 0, NULL);
    EIM_check_error("Retrieving vla long-string sizes\n");

    if (report != NULL)
    {
        for (row = report; row != NULL; row = row->next)
        {
            char* tmp_puid = NULL;
            EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);
            puids.push_back(tmp_puid);

            int* tmp_size = NULL;
            EIM_find_value(headers, row->line, "pseq", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                pseqs.push_back(*tmp_size);
            }
            else
            {
                pseqs.push_back(0);
            }

            tmp_size = NULL;
            EIM_find_value(headers, row->line, "stored_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                stored_sizes.push_back(*tmp_size);
            }
            else
            {
                stored_sizes.push_back(0);
            }

            tmp_size = NULL;
            EIM_find_value(headers, row->line, "calc_size", EIM_integer, &tmp_size);

            if (tmp_size != NULL)
            {
                calc_sizes.push_back(*tmp_size);
            }
            else
            {
                calc_sizes.push_back(0);
            }
        }
        EIM_free_result(headers, report);
        report = NULL;
        headers = NULL;
    }

    if (!trans_was_active)
    {
        EIM_commit_transaction("get_long_string_sizes()");
    }

    ERROR_RECOVER

    const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

    return(ifail);
}

/*------------------------------------------------------------------------
** Validate that the actual string sizes match the stored string sizes
** and optionally correct the stored string sizes.
** The input calculated sizes must be in assending order.
** ----------------------------------------------------------------------- */
static int validate_vla_long_string_sizes(const char* func, const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = OK;
    // RM_SLV_ stands for Reference Manager String Length Validation
    int rm_slv_batch_size = 65500;       // Number of records processed per batch
    int rm_slv_max_len = 200000;         // Maximum length of records within the batch
    int rm_slv_percent_exp = 30;         // Percentage of expected expansion due to none single byte characters
    int rm_slv_fixed_exp = 100;          // Additional fixed byte expansion to provide additional memory for unforseen expansions...
    int rm_slv_max_mem_m = 6000;         // Maximum memory to be used within each batch... unit single record processing kicks in.
    long rm_slv_max_int = 2147483647;    // Maximum integer value
    int64_t rm_slv_meg = 1024 * 1024;       // Megabyte

    int rpt_worked_cnt = 0;              // Number of records that were processed
    int rpt_success_cnt = 0;             // Number of records where data length matches the stored length
    int rpt_under_cnt = 0;               // Number of records where the stored count under represents the data length
    int rpt_over_cnt = 0;                // Number of records where the stored count over represents the data length
    int rpt_unknown_cnt = 0;             // Number of records that we don't know about (NULL for size returned etc...)
    int rpt_eligible_cnt = 0;            // Number of records that are eligible for correction
    int rpt_corrected_cnt = 0;           // Number of records where the stored count was actually corrected

    int wrk_start_pos = 0;               // UID to start with.
    int wrk_batch_size = 0;              // Batch size for the current SQL statement
    int wrk_max_len = 0;                 // Max data length for this SQL statement on this platform
    int wrk_max_calc = 0;                // The maximum caculated size for this batch

                                         // Check configuration values. 
    rm_slv_batch_size  = DDS_ask_pom_parameter_int("rm_slv_batch_size",  rm_slv_batch_size);
    rm_slv_max_len     = DDS_ask_pom_parameter_int("rm_slv_max_len",     rm_slv_max_len);
    rm_slv_percent_exp = DDS_ask_pom_parameter_int("rm_slv_percent_exp", rm_slv_percent_exp);
    rm_slv_fixed_exp   = DDS_ask_pom_parameter_int("rm_slv_fixed_exp",   rm_slv_fixed_exp);
    rm_slv_max_mem_m   = DDS_ask_pom_parameter_int("rm_slv_max_mem_m",   rm_slv_max_mem_m);
    EIM_uid_t* cor_puids = NULL;
    int*       cor_pseqs = NULL;
    int*       cor_lens = NULL;

    int storge_bytes_per_ascii_char = 2;    // For unicode there are two bytes for every ascii character

    if (!EIM_unicode_enabled())
    {
        storge_bytes_per_ascii_char = 1;    // If not unicode we assume UTF-8, in which case we have 1 storage byte per ascii character
    }

    // Filter to ensure that we output bad uids only once into the syslog. 
    std::map< std::string, int > dups;
    std::map< std::string, int >::iterator it;

    // As we are currently not using ENQ or temporary tables we will limit the batch size to 200 to limit the length of the SQL statement.
    if (rm_slv_batch_size > 200)
    {
        rm_slv_batch_size = 200;
    }

    // Make sure we limit the batch size so that we don't use significantly more memory than the maximum configured.
    int64_t temp_cnt = rm_slv_meg * rm_slv_max_mem_m / rm_slv_max_len;

    if (temp_cnt < rm_slv_batch_size)
    {
        rm_slv_batch_size = (int)temp_cnt;
    }

    if (calc_sizes.size() < rm_slv_batch_size)
    {
        rm_slv_batch_size = calc_sizes.size();
    }

    if (rm_slv_batch_size < 1)
    {
        rm_slv_batch_size = 1;
    }

    cor_puids = (EIM_uid_t*)SM_alloc(sizeof(EIM_uid_t) * (rm_slv_batch_size + 1));
    cor_pseqs = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));
    cor_lens = (int*)SM_alloc(sizeof(int) * (rm_slv_batch_size + 1));

    logical trans_was_active = true;
    EIM_select_var_t vars[4];
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;

    ERROR_PROTECT

    str_len_val_status(true, "Start ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

    while (wrk_start_pos < puids.size())
    {
        trans_was_active = true;

        if (!EIM_is_transaction_active())
        {
            trans_was_active = false;
            EIM_start_transaction();
        }

        int cor_offset = 0;

        // Any time a record is larger that rm_slv_max_len, we want to set the batch size to one. 
        // So look check through the next batch of values for the first large data value. 

        wrk_batch_size = 1;
        wrk_max_calc = 0;

        if (rm_slv_batch_size > 1 && wrk_start_pos < puids.size())
        {
            int term_pos = wrk_start_pos;

            for (int j = 0; j < rm_slv_batch_size && term_pos < puids.size(); j++, term_pos++)
            {
                if (calc_sizes[wrk_start_pos + j] / storge_bytes_per_ascii_char > rm_slv_max_len)
                {
                    break;
                }

                if (calc_sizes[wrk_start_pos + j] > wrk_max_calc)
                {
                    wrk_max_calc = calc_sizes[wrk_start_pos + j];
                }
            }

            if (term_pos - wrk_start_pos > 1)
            {
                wrk_batch_size = term_pos - wrk_start_pos;
            }
        }

        if (wrk_batch_size == 1)
        {
            rm_slv_batch_size = 1;
        }

        if (wrk_batch_size > 0)
        {
            // Calculate max buffer size based on configured expected expansion ratio.
            // Start with the largest cacluate size in our batch.
            long tmp_max_len = wrk_max_calc;

            if (wrk_batch_size == 1)
            {
                // If the batch size is just one then use the calcualted from for this record.
                tmp_max_len = calc_sizes[wrk_start_pos];
            }

            if (EIM_unicode_enabled() && EIM_dbplat() == EIM_dbplat_mssql)
            {
                tmp_max_len = tmp_max_len >> 1;
                tmp_max_len++;
            }

            tmp_max_len = tmp_max_len + (long)(tmp_max_len * rm_slv_percent_exp / 100) + rm_slv_fixed_exp;

            if (tmp_max_len > rm_slv_max_int)
            {
                wrk_max_len = (int)rm_slv_max_int;
            }
            else
            {
                wrk_max_len = (int)tmp_max_len;
            }

            // Build SQL to retrieve actual data from the database
            std::stringstream sql;

            sql << "SELECT a.puid puid, a.pseq pseq, a." << att_col << " data, a.pvall stored_size";
            sql << " FROM " << att_tbl << " a WHERE";

            for (int i = 0; i < wrk_batch_size; i++)
            {
                if (i > 0)
                {
                    sql << " OR";
                }

                sql << " (a.puid = '" << puids[wrk_start_pos] << "' AND a.pseq = " << pseqs[wrk_start_pos] << ")";
                wrk_start_pos++;
            }

            EIM_select_col(&(vars[0]), EIM_puid, "puid", EIM_uid_length + 1, false);
            EIM_select_col(&(vars[1]), EIM_integer, "pseq", sizeof(int), false);
            EIM_select_col(&(vars[2]), EIM_varchar, "data", wrk_max_len, false);
            EIM_select_col(&(vars[3]), EIM_integer, "stored_size", sizeof(int), false);

            ifail = EIM_exec_sql_bind(sql.str().c_str(), &headers, &report, 0, 4, vars, 0, NULL);
            EIM_check_error("Retrieving long-string data\n");

            if (report != NULL)
            {
                for (row = report; row != NULL; row = row->next)
                {
                    rpt_worked_cnt++;

                    char* tmp_puid = NULL;
                    EIM_find_value(headers, row->line, "puid", EIM_puid, &tmp_puid);

                    int* tmp_pseq = NULL;
                    EIM_find_value(headers, row->line, "pseq", EIM_integer, &tmp_pseq);

                    if (tmp_puid == NULL || tmp_pseq == NULL)
                    {
                        rpt_unknown_cnt++;
                        continue;
                    }

                    char* tmp_data = NULL;
                    int str_len = 0;
                    logical correction_required = false;

                    EIM_find_value(headers, row->line, "data", EIM_varchar, &tmp_data);

                    if (tmp_data != NULL)
                    {
                        str_len = strlen(tmp_data);
                    }

                    int *tmp_size = NULL;
                    EIM_find_value(headers, row->line, "stored_size", EIM_integer, &tmp_size);

                    if (tmp_size != NULL)
                    {
                        if (*tmp_size < str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_under_cnt++;
                        }
                        else if (*tmp_size > str_len)
                        {
                            correction_required = true;
                            rpt_eligible_cnt++;
                            rpt_over_cnt++;
                        }
                        else
                        {
                            correction_required = false;
                            rpt_success_cnt++;
                        }
                    }
                    else
                    {
                        correction_required = true;
                        rpt_eligible_cnt++;
                        rpt_unknown_cnt++;
                    }

                    if (correction_required)
                    {
                        //  resolves coverity defect 506020 : String Overflow
                        if (sizeof(cor_puids[cor_offset]) > strlen(tmp_puid))
                        {
                            strcpy(cor_puids[cor_offset], tmp_puid);
                        }
                        else
                        {
                            ERROR_internal(ERROR_line, "Size not enough to copy '%s' ", tmp_puid);
                        }

                        cor_pseqs[cor_offset] = *tmp_pseq;
                        cor_lens[cor_offset] = str_len;
                        cor_offset++;
                    }
                }
                EIM_free_result(headers, report);
                report = NULL;
                headers = NULL;
            }

            if (cor_offset > 0 && DDS_is_pom_parameter_enabled("RM_STR_LEN_LOG_BAD_UIDS", true))
            {
                int counter = 0;

                do
                {
                    logical valid_uids = false;
                    {
                        std::stringstream msg;
                        msg << "\nreference_manager -str_len_val -u=user -p=password -g=dba -from=" << cls << ":" << attr << " -commit";

                        for (int i = 0; i < 20 && counter < cor_offset; i++, counter++)
                        {
                            std::string temp_uid = cor_puids[counter];
                            it = dups.find(temp_uid);

                            if (it == dups.end())
                            {
                                valid_uids = true;
                                dups.insert(std::pair< std::string, int>(temp_uid, 0));
                                msg << " -uid=" << (const char*)(cor_puids[counter]);
                            }
                        }
                        msg << "\n";

                        if (valid_uids)
                        {
                            valid_uids = false;
                            logger()->printf(msg.str().c_str());
                        }
                    }
                } while (counter < cor_offset);
            }

            if (cor_offset > 0 && args->commit_flag)
            {
                // 
                // Update data lengths here. 
                //
                ifail = bulk_update_vla_integers(att_tbl, "pvall", cor_puids, cor_pseqs, cor_lens, cor_offset);
                rpt_corrected_cnt += cor_offset;
            }
        }

        str_len_val_status(false, NULL, func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);

        if (!trans_was_active)
        {
            EIM_commit_transaction("validate_long_string_sizes()");
        }
    }

    ERROR_RECOVER

        const std::string msg("EXCEPTION: See syslog for additional details.");
    cons_out(msg);

    if (!trans_was_active)
    {
        EIM__clear_transaction(ERROR_ask_failure_code());
        ERROR_raise(ERROR_line, EIM_ask_abort_code(), "Failed to execute the query\n");
    }
    else
    {
        ERROR_reraise();
    }
    ERROR_END

        SM_free(cor_puids);
    SM_free(cor_pseqs);
    SM_free(cor_lens);

    *bad_count = rpt_eligible_cnt;
    str_len_val_status(true, "Final ", func, puids.size(), rpt_worked_cnt, rpt_under_cnt, rpt_over_cnt, rpt_success_cnt, rpt_unknown_cnt, rpt_eligible_cnt, rpt_corrected_cnt);
    return(ifail);
}

/*------------------------------------------------------------------------
** Bulk update integers for specified VLA tables, UIDs.
** ----------------------------------------------------------------------- */
static int bulk_update_vla_integers(const char *tbl, const char *col, EIM_uid_t *uids, int* seqs, int* ints, int size)
{
    int ifail = OK;

    if (size < 1)
    {
        return(ifail);
    }

    const int maxUpdateSize = EIM_get_max_insert_size();
    int         batch_size = 0;
    EIM_uid_t*  uid_ptr = uids;
    int*        seq_ptr = seqs;
    int*        int_ptr = ints;

    if (maxUpdateSize < size)
    {
        batch_size = maxUpdateSize;
        if (batch_size < 1)
            batch_size = EIM_ARRAY_MAX_SIZE;
    }
    else
    {
        batch_size = size;
    }

    // Allocate an array of pointers to bind array value pointers.
    EIM_bind_array_value_p_t bvs[3];
    bvs[0] = static_cast<EIM_bind_array_value_p_t>(SM_alloc(sizeof(EIM_bind_array_value_t)));
    bvs[1] = static_cast<EIM_bind_array_value_p_t>(SM_alloc(sizeof(EIM_bind_array_value_t)));
    bvs[2] = static_cast<EIM_bind_array_value_p_t>(SM_alloc(sizeof(EIM_bind_array_value_t)));

    bvs[0]->type = EIM_integer;
    bvs[0]->len = sizeof(int);
    bvs[0]->array_size = 0;
    bvs[0]->ind = (short *)SM_calloc(batch_size, sizeof(short));
    bvs[0]->value = NULL;

    bvs[1]->type = EIM_puid;
    bvs[1]->len = sizeof(EIM_uid_t);
    bvs[1]->array_size = 0;
    bvs[1]->ind = (short *)SM_calloc(batch_size, sizeof(short));
    bvs[1]->value = NULL;

    bvs[2]->type = EIM_integer;
    bvs[2]->len = sizeof(int);
    bvs[2]->array_size = 0;
    bvs[2]->ind = (short *)SM_calloc(batch_size, sizeof(short));
    bvs[2]->value = NULL;

    int i = 0;
    int this_batch = size;
    this_batch = this_batch % batch_size;

    if (this_batch == 0)
    {
        this_batch = batch_size;
    }

    while (i < size)
    {
        bvs[0]->array_size = this_batch;
        bvs[0]->value = int_ptr + i;

        bvs[1]->array_size = this_batch;
        bvs[1]->value = uid_ptr + i;

        bvs[2]->array_size = this_batch;
        bvs[2]->value = seq_ptr + i;

        char* sql = SM_sprintf("UPDATE %s SET %s = :1 WHERE puid = :2 AND pseq = :3", tbl, col);

        // Do update
        ifail = EIM_exec_imm_array_bind(sql, "bulk_update_vla_integers(): bulk updating vla string lengths", this_batch, 3, bvs);
        EIM_check_error("bulk_update_vla_integers(): bulk updating vla string lengths");

        SM_free(sql);

        i = i + this_batch;
        this_batch = batch_size;

        bvs[0]->array_size = 0;
        bvs[0]->value = NULL;

        bvs[1]->array_size = 0;
        bvs[1]->value = NULL;

        bvs[2]->array_size = 0;
        bvs[2]->value = NULL;
    }

    if (i != size)
    {
        ERROR_raise(ERROR_line, POM_internal_error, "validate_long_string_sizes(): Not all data written to %s table, missing at least %d records. (%d)", tbl, size - i, ifail);
    }

    SM_free(bvs[0]->ind);
    SM_free(bvs[1]->ind);
    SM_free(bvs[2]->ind);
    SM_free(bvs[0]);
    SM_free(bvs[1]);
    SM_free(bvs[2]);

    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted and stored sizes for long string data.
** Data is returned in ascending order by calculated size.
** ----------------------------------------------------------------------- */
static int get_la_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes)
{
    int ifail = get_vla_long_string_sizes(att_tbl, att_col, uid, uid_vec, puids, pseqs, calc_sizes, stored_sizes);
    return(ifail);
}
    
/*------------------------------------------------------------------------
** Validate that the actual string sizes match the stored string sizes
** and optionally correct the stored string sizes.
** The input calculated sizes must be in assending order.
** ----------------------------------------------------------------------- */
static int validate_la_long_string_sizes(const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = validate_vla_long_string_sizes("LSt(LA)", cls, attr, att_tbl, att_col, puids, pseqs, calc_sizes, bad_count);
    return(ifail);
}

/*------------------------------------------------------------------------
** Retrieves the calcualted and stored sizes for long string data.
** Data is returned in ascending order by calculated size.
** ----------------------------------------------------------------------- */
static int get_sa_long_string_sizes(const char *att_tbl, const char *att_col, const char* uid, std::vector< std::string >  *uid_vec, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, std::vector< int > &stored_sizes)
{
    int ifail = get_vla_long_string_sizes(att_tbl, att_col, uid, uid_vec, puids, pseqs, calc_sizes, stored_sizes);
    return(ifail);
}

/*------------------------------------------------------------------------
** Validate that the actual string sizes match the stored string sizes
** and optionally correct the stored string sizes.
** The input calculated sizes must be in assending order.
** ----------------------------------------------------------------------- */
static int validate_sa_long_string_sizes(const char* cls, const char* attr, const char *att_tbl, const char *att_col, std::vector< std::string > &puids, std::vector< int > &pseqs, std::vector< int > &calc_sizes, int* bad_count)
{
    int ifail = validate_vla_long_string_sizes("LSt(SA)", cls, attr, att_tbl, att_col, puids, pseqs, calc_sizes, bad_count);
    return(ifail);
}

/* ********************************************************************************
** END OF: str_len_val_op() routines.
** *******************************************************************************/

/*------------------------------------------------------------------------
** Validate that the system is configured for database transactions
** and return op-not-supported if not configued. 
** ----------------------------------------------------------------------- */
static int database_tx_check()
{
    int ifail = OK;

#ifndef PRE_TC131_PLATFORM
    if (!Teamcenter::POMUtilityTransaction::IsDbTxEnabled())
    {
        ifail = error_out(ERROR_line, POM_op_not_supported, "Ref_Mgr requires database transactions, remove the TC_DISABLE_UTIL_TRANSACTIONS environment variable.");
    }
#else
    if (getenv("TC_DISABLE_TRANSACTIONS") != NULL)
    {
        ifail = error_out(ERROR_line, POM_op_not_supported, "Ref_Mgr requires database transactions, temporarily remove the TC_DISABLE_TRANSACTIONS environment variable from this session.");
    }
#endif
    return(ifail);
}

/*-----------------------------------------------------------------------*/
/* Outputs the POM_stub details assocaited with the specfied object uid. */
static void output_stub_details( std::vector<std::string>* uid_vec, int* found_count )
{
    *found_count = -1;
    int actual_found = 0;
    logical trans_was_active = true;

    if ( !EIM_is_transaction_active( ) )
    {
        trans_was_active = false;
        EIM_start_transaction( );
    }

    cons_out( "" );

    ERROR_PROTECT

    for ( int i = 0; i < uid_vec->size( ); i++ )
    {
        int ifail = OK;
        EIM_select_var_t vars[8];
        EIM_value_p_t headers = NULL;
        EIM_row_p_t report = NULL;
        EIM_row_p_t row;
        int row_cnt = 0;

        const char* sql = "SELECT puid, pobject_uid, pobject_class, pobject_id, pobject_name, pobject_desc, powning_user_id, pstatus_flag FROM PPOM_STUB WHERE pobject_uid = :1";

        EIM_select_col( &(vars[0]), EIM_puid, "puid", MAX_UID_SIZE, false );
        EIM_select_col( &(vars[1]), EIM_puid, "pobject_uid", 34, false );
        EIM_select_col( &(vars[2]), EIM_varchar, "pobject_class", 30, true );
        EIM_select_col( &(vars[3]), EIM_varchar, "pobject_id", 258, true );
        EIM_select_col( &(vars[4]), EIM_varchar, "pobject_name", 258, true );
        EIM_select_col( &(vars[5]), EIM_varchar, "pobject_desc", 242, true );
        EIM_select_col( &(vars[6]), EIM_varchar, "powning_user_id", 130, true );
        EIM_select_col( &(vars[7]), EIM_varchar, "pstatus_flag", sizeof( int ), true );

        EIM_bind_var_t bindVars[1];
        EIM_bind_val( &bindVars[0], EIM_puid, strlen( uid_vec->at( i ).c_str( ) ) + 1, (uid_vec->at( i ).c_str( )) );
        ifail = EIM_exec_sql_bind( sql, &headers, &report, 0, 8, vars, 1, bindVars );

        if ( !args->ignore_errors_flag )
        {
            EIM_check_error( "output_stub_details()\n" );
        }
        else if ( ifail != OK )
        {
            EIM_clear_error( );
            EIM_free_result( headers, report );
            report = NULL;
            headers = NULL;

            std::stringstream msg;
            msg << uid_vec->at( i ) << ":  Error while quering the database";
            cons_out( msg.str( ) );
        }

        if ( report != NULL )
        {
            std::stringstream msg;
            row_cnt = 0;

            for ( row = report; row != NULL; row = row->next )
                row_cnt++;

            row = report;
            msg << uid_vec->at( i ) << ":\n";

            for ( int j = 0; j < row_cnt; j++, actual_found++ )
            {

                char* ptr = NULL;
                EIM_find_value( headers, row->line, "puid", EIM_puid, &ptr );
                msg << "  " << actual_found + 1 << "  uid            = " << ptr << "\n";

                ptr = NULL;
                EIM_find_value( headers, row->line, "pobject_uid", EIM_puid, &ptr );
                msg << "     object_uid     = " << ptr << "\n";

                ptr = NULL;
                EIM_find_value( headers, row->line, "pobject_class", EIM_varchar, &ptr );

                if ( ptr == NULL )
                {
                    msg << "     object_class   = NULL\n";
                }
                else
                {
                    msg << "     object_class   = " << ptr << "\n";
                }

                ptr = NULL;
                EIM_find_value( headers, row->line, "pobject_id", EIM_varchar, &ptr );

                if ( ptr == NULL )
                {
                    msg << "     object_id      = NULL\n";
                }
                else
                {
                    msg << "     object_id      = " << ptr << "\n";
                }

                ptr = NULL;
                EIM_find_value( headers, row->line, "pobject_name", EIM_varchar, &ptr );

                if ( ptr == NULL )
                {
                    msg << "     object_name    = NULL\n";
                }
                else
                {
                    msg << "     object_name    = " << ptr << "\n";
                }

                ptr = NULL;
                EIM_find_value( headers, row->line, "pobject_desc", EIM_varchar, &ptr );

                if ( ptr == NULL )
                {
                    msg << "     object_desc    = NULL\n";
                }
                else
                {
                    msg << "     object_desc    = " << ptr << "\n";
                }

                ptr = NULL;
                EIM_find_value( headers, row->line, "powning_user_id", EIM_varchar, &ptr );

                if ( ptr == NULL )
                {
                    msg << "     owning_user_id = NULL\n";
                }
                else
                {
                    msg << "     owning_user_id = " << ptr << "\n";
                }

                int* int_ptr = NULL;
                EIM_find_value( headers, row->line, "pstatus_flag", EIM_integer, &int_ptr );

                if ( int_ptr == NULL )
                {
                    msg << "     status_flag    = NULL\n";
                }
                else
                {
                    msg << "     status_flag    = " << *int_ptr << "\n";
                }

                row = row->next;
            }
            cons_out( msg.str( ) );
        }
        else
        {
            std::stringstream msg;
            msg << uid_vec->at( i ) << ":       No stub found\n";
            cons_out( msg.str( ) );
        }

        EIM_free_result( headers, report );
        report = NULL;
        headers = NULL;
    }

    *found_count = actual_found;

    ERROR_RECOVER
    const std::string msg( "EXCEPTION: See syslog for additional details. (See -i option to ignore this error.)" );
    cons_out( msg );

    if ( !trans_was_active )
    {
        EIM__clear_transaction( ERROR_ask_failure_code( ) );
        ERROR_raise( ERROR_line, EIM_ask_abort_code( ), "Failed to execute the query\n" );
    }
    else
    {
        ERROR_reraise( );
    }
    ERROR_END

    if ( !trans_was_active )
    {
        EIM_commit_transaction( "output_stub_details()" );
    }
}

/* ********************************************************************************
** New utility routines.
** *******************************************************************************/
static logical isReferenceable( cls_t* meta )
{
    logical ret = true;

    if ( ( meta->properties & OM_class_prop_class_not_referenceable ) == OM_class_prop_class_not_referenceable )
    {
        ret = false;
    }
    return ret;
}

static logical isVersionable( cls_t* meta )
{
    logical ret = false;

    if ( ( meta->properties & ( OM_class_prop_is_versionable + OM_class_prop_can_have_versionable_subclasses ) ) > 0 )
    {
        ret = true;
    }
    return ret;
}

static logical isVersionable( std::vector<cls_t>& meta, const char* class_name )
{
    logical ret = false;
    logical found = false;

    for ( int i = 0; i < meta.size( ); i++ )
    {
        if ( strcmp( meta[i].name, class_name ) == 0 )
        {
            ret = isVersionable( &meta[i] );
            found = true;
            break;
        }
    }

    if ( !found )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "An invalid class name was specified (%s)", class_name );
    }
    return ret;
}



static void filterOutFlatClasses( std::vector<cls_t>& meta, std::vector<minny_meta_t>& flat_classes )
{
    flat_classes.clear();

    minny_meta_t flat_cls;

    for ( int i = 0; i < meta.size(); i++ )
    {
        if ( isFlat( &meta[i] ) )
        {
            flat_cls.cls_id = meta[i].cls_id;
            strncpy (flat_cls.name, meta[i].name, CLS_NAME_SIZE);
            flat_cls.name[CLS_NAME_SIZE] = '\0';
            strncpy( flat_cls.db_name, meta[i].db_name, CLS_DB_NAME_SIZE );
            flat_cls.db_name[CLS_DB_NAME_SIZE] = '\0';
            flat_cls.cls_pos = i;
            flat_cls.count = 0;
            flat_classes.push_back(flat_cls);
        }  
    }
}

static void get_int_from_sql(const char* sql, const char* column_name, int* first_row_value, int* row_count)
{
    *first_row_value = -1;
    *row_count = 0;

    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    EIM_select_var_t vars[1];
    EIM_select_col(&(vars[0]), EIM_integer, column_name, sizeof(int), false);

    EIM_exec_sql_bind(sql, &headers, &report, "Getting integer value from SQL", 1, vars, 0, NULL);

    if (report != NULL)
    {
        int row_cnt = 0;

        for (row = report; row != NULL; row = row->next)
        {
            row_cnt++;
        }

        *row_count = row_cnt;

        int* value = NULL;
        EIM_find_value(headers, report->line, column_name, EIM_integer, (void**)& value);

        if (value != NULL)
        {
            *first_row_value = *value;
        }
    }

    EIM_free_result(headers, report);
}

static int get_row_count( const char* table_name )
{
    char* sql = SM_sprintf( "SELECT COUNT(*) AS CNT FROM %s", table_name );

    int row_cnt = 0;
    int rows_retieved = 0;

    get_int_from_sql( sql, "CNT", &row_cnt, &rows_retieved );
    SM_free( sql );

    return row_cnt;
}

// Create a where clause ( = or IN) for the specified UIDs. 
// Return value should be freed by the caller. 
static char* create_uid_specific_where_clause( const char* sql_prefix, std::vector<std::string>* uids )
{
    if ( uids == NULL )
    {
        return NULL;
    }

    int size = uids->size();

    if ( size < 1 )
    {
        return NULL;
    }

    if ( size > 100 )
    {
        ERROR_raise( ERROR_line, POM_invalid_value, "The maximum number of UIDs that is currently supported is 100. (%d) Please reduce the number of specified UIDs.", size );
    }

    std::stringstream where_clause;
    where_clause << sql_prefix;

    if ( size == 1 )
    {
        where_clause << " = '" << (*uids)[0] << "'";
    }
    else
    {
        where_clause << " IN ('" << (*uids)[0] << "' ";

        for ( int i = 1; i < size; i++ )
        {
            where_clause << ",'" << (*uids)[i] << "'";
        }

        where_clause << ")";
    }

    return ( SM_string_copy( where_clause.str().c_str() ) );
}

/* ********************************************************************************
** START OF: remove_unneeded_bp_op() (RUB) routines. 
** *******************************************************************************/
static char* tbl_unneeded_from_uid = NULL;
static char* tbl_unneeded_to_uid = NULL;

static const char* idx_unneeded_from_bkptrs = "RM1_I_RUB_UNNEEDED_FROM_UID";
static const char* idx_unneeded_to_bkptrs = "RM1_I_RUB_UNNEEDED_TO_UID";

static const char* aoid_col_name = "aoid";
static const char* puid_col_name = "puid";
static const char* from_uid_col_name = "from_uid";
static const char* to_uid_col_name = "to_uid";

/* 
** Returns the class names of flattened classes that are referenced from backpointers.
** A return value of true indicates that there are some references that contain -1 for its class ID, or otherwise it's class is unknown.
*/
static logical RUB_get_flat_classes_with_bps( std::vector<minny_meta_t>& flat_classes, const char* temp_table, const char* column_name, std::vector<minny_meta_t>& flat_classes_with_bps )
{
    const char* class_column = "XXXX";

    if ( strcmp( column_name, from_uid_col_name ) == 0 )
    {
        class_column = "from_class";
    }
    else if ( strcmp( column_name, to_uid_col_name ) == 0 )
    {
        class_column = "to_class";
    }
    else
    {
        ERROR_raise( ERROR_line, POM_internal_error, "RUB_get_flat_classes_with_bps() - Invalid column name (%s)", column_name );
    }

    char* sql = SM_sprintf( "SELECT b.%s, count(*) AS cnt FROM %s a INNER JOIN POM_BACKPOINTER b ON a.%s = b.%s GROUP BY %s",
                            class_column, temp_table, column_name, column_name, class_column );

    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    EIM_select_var_t vars[2];
    EIM_select_col( &( vars[0] ), EIM_integer, class_column, sizeof( int ), false );
    EIM_select_col( &( vars[1] ), EIM_integer, "cnt", sizeof( int ), false );

    EIM_exec_sql_bind( sql, &headers, &report, "Getting classes from backpointer references", 2, vars, 0, NULL );

    flat_classes_with_bps.clear();

    logical unknown_class = false;

    if ( report != NULL )
    {
        for ( row = report; row != NULL; row = row->next )
        {
            // class column
            int* cls_ptr = NULL;
            EIM_find_value( headers, row->line, class_column, EIM_integer, &cls_ptr );

            if ( cls_ptr == NULL || *cls_ptr < 0 )
            {
                unknown_class = true;
                break; // We are going to need to look in all flattened classes.
            }

            int* cnt_ptr = NULL;
            int cnt = -1;
            EIM_find_value( headers, row->line, "cnt", EIM_integer, &cnt_ptr );

            if ( cnt_ptr )
            {
                cnt = *cnt_ptr;
            }

            for ( int i = 0; i < flat_classes.size(); i++ )
            {
                // Is this a flattened class.
                if ( flat_classes[i].cls_id == *cls_ptr )
                {
                    int pos = -1;

                    for ( int j = 0; i < flat_classes_with_bps.size(); i++ )
                    {
                        // Have we seen this class before?
                        if ( flat_classes_with_bps[i].cls_id == *cls_ptr )
                        {
                            pos = j;
                            break;
                        }
                    }

                    if ( pos == -1 )
                    {
                        // We've not seen this class yet.
                        int saved_count = flat_classes[i].count;
                        flat_classes[i].count = cnt;                      
                        flat_classes_with_bps.push_back( flat_classes[i] );
                        flat_classes[i].count = saved_count;
                    }
                    break;
                }
            }
        }
    }
    SM_free( sql );
    EIM_free_result( headers, report );
    headers = NULL;
    report = NULL;

    return unknown_class;
}

static int RUB_create_temporary_tables( logical to_uid_processing )
{
    int ifail = OK;
    int lfail = OK;

    int puid_col_type = POM_string;
    int puid_col_len = EIM_uid_length;

    logical trans_was_active = true;

    if ( !EIM_is_transaction_active())
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    tbl_unneeded_from_uid = NULL;
    tbl_unneeded_to_uid = NULL;

    if ( !to_uid_processing )
    {

        lfail = POM_create_table( POM_TEMPORARY_TABLE, "RM1_", "RUB_UNNEEDED_FROM_UID", 1, &from_uid_col_name, &puid_col_type, &puid_col_len, POM_TT_CLEAR_ROWS_EOS, &tbl_unneeded_from_uid );

        if ( lfail != OK )
        {
            logger()->printf( "Unable to create temporary table for RM1_RUB_UNNEEDED_FROM_UID (ifail = %d)", lfail );

            if ( ifail == OK )
            {
                ifail = lfail;
            }
        }
    }
    else
    {
        lfail = POM_create_table( POM_TEMPORARY_TABLE, "RM1_", "RUB_UNNEEDED_TO_UID", 1, &to_uid_col_name, &puid_col_type, &puid_col_len, POM_TT_CLEAR_ROWS_EOS, &tbl_unneeded_to_uid );

        if ( lfail != OK )
        {
            logger()->printf( "Unable to create temporary table for RM1_RUB_UNNEEDED_TO_UID (ifail = %d)", lfail );

            if ( ifail == OK )
            {
                ifail = lfail;
            }
        }
    }

    if ( ifail != OK )
    {
        std::stringstream msg;
        msg << "Unable to create at least one temporary table, see syslog for additional information.\n";

        if ( !to_uid_processing )
        {
            if ( tbl_unneeded_from_uid )
            {
                msg << "  Table name for unneeded from_uids = " << *tbl_unneeded_from_uid << "\n";
            }
            else
            {
                msg << "  Table name for unneeded from_uids = <NULL>\n";
            }
        }
        else
        {
            if ( tbl_unneeded_to_uid )
            {
                msg << "  Table name for unneeded to_uids = " << *tbl_unneeded_to_uid;
            }
            else
            {
                msg << "  Table name for unneeded to_uids = <NULL>";
            }
        }

        cons_out( msg.str() );
    }

    if ( !trans_was_active )
    {
        EIM_commit_transaction( "RUB_create_temporary_tables()" );
    }

    return ifail;
}

static int RUB_dml_or_ddl( const char* sql )
{
    if ( EIM_exec_imm_bind( sql, NULL, 0, NULL ) )
    {
        lprintf( "RefMgr RUB_dml_or_ddl failure: %s\n", sql );
        EIM_check_error( "RUB_dml_or_ddl()" );
    }

    int rows_processed = EIM_sqlca_rows_returned();

    if ( rows_processed < 0)
    {
        rows_processed = 0;
    }

    return (rows_processed);
}


static void RUB_create_temporary_table_index( const char* index_name, const char* table_name, const char* column_name)
{
    if (index_name != NULL && table_name != NULL && column_name != NULL)
    {
        int count = -1;
        int rows = 0;
        char* sql = NULL;

        switch (EIM_dbplat())
        {
        case EIM_dbplat_oracle:
            sql = SM_sprintf("SELECT count(*) AS CNT FROM user_indexes WHERE table_name = '%s'", table_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count < 1 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("CREATE UNIQUE INDEX %s ON %s (%s)", index_name, table_name, column_name);
                RUB_dml_or_ddl(sql);
            }
            break;

        case EIM_dbplat_mssql:
            sql = SM_sprintf("SELECT count(*) AS CNT from tempdb.sys.indexes WHERE name = '%s'", index_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count < 1 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("CREATE UNIQUE INDEX %s ON %s ([%s])", index_name, table_name, column_name);
                RUB_dml_or_ddl(sql);
            }
            break;

        case EIM_dbplat_postgres:
            sql = SM_sprintf("SELECT count(*) AS CNT FROM pg_indexes WHERE tablename = '%s'", table_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count < 1 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("CREATE UNIQUE INDEX %s ON %s (%s)", index_name, table_name, column_name);
                RUB_dml_or_ddl(sql);
            }
            break;

        default:
            ERROR_raise(ERROR_line, POM_internal_error, "Unsupported database platform");
            break;
        }
    }
}

static void RUB_create_temporary_table_indexes()
{
    logical trans_was_active = true;

    if (!EIM_is_transaction_active())
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    RUB_create_temporary_table_index(idx_unneeded_from_bkptrs, tbl_unneeded_from_uid, from_uid_col_name);
    RUB_create_temporary_table_index(idx_unneeded_to_bkptrs, tbl_unneeded_to_uid, to_uid_col_name);

    if (!trans_was_active)
    {
        EIM_commit_transaction("RUB_create_temporary_table_indexes()");
    }
}

static void RUB_drop_temporary_table_index(const char* index_name, const char* table_name )
{
    if (index_name != NULL && table_name != NULL)
    {
        int count = -1;
        int rows = 0;
        char* sql = NULL;

        switch (EIM_dbplat())
        {
        case EIM_dbplat_oracle:
            sql = SM_sprintf("SELECT count(*) AS CNT FROM user_indexes WHERE table_name = '%s' AND index_name = '%s'", table_name, index_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count > 0 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("DROP INDEX %s", index_name );
                RUB_dml_or_ddl(sql);
            }
            break;

        case EIM_dbplat_mssql:
            sql = SM_sprintf("SELECT count(*) AS CNT from tempdb.sys.indexes WHERE name = '%s'", index_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count > 0 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("DROP INDEX %s ON %s", index_name, table_name);
                RUB_dml_or_ddl(sql);
            }
            break;

        case EIM_dbplat_postgres:
            sql = SM_sprintf("SELECT count(*) AS CNT FROM pg_indexes WHERE tablename = '%s' AND indexname = '%s'", table_name, index_name);
            get_int_from_sql(sql, "CNT", &count, &rows);

            if (count >0 && rows > 0)
            {
                SM_free(sql);
                sql = SM_sprintf("DROP INDEX %s", index_name);
                RUB_dml_or_ddl(sql);
            }
            break;

        default:
            ERROR_raise(ERROR_line, POM_internal_error, "Unsupported database platform");
            break;
        }
    }
}

static void RUB_drop_temporary_table_indexes()
{
    logical trans_was_active = true;

    if (!EIM_is_transaction_active())
    {
        trans_was_active = false;
        EIM_start_transaction();
    }

    RUB_drop_temporary_table_index(idx_unneeded_from_bkptrs, tbl_unneeded_from_uid);
    RUB_drop_temporary_table_index(idx_unneeded_to_bkptrs, tbl_unneeded_to_uid);

    if (!trans_was_active)
    {
        EIM_commit_transaction("RUB_drop_temporary_table_indexes()");
    }
}


static void RUB_clear_temp_tables()
{
    if ( tbl_unneeded_from_uid != NULL )
    {
        POM_clear_table( tbl_unneeded_from_uid );
    }

    if ( tbl_unneeded_to_uid != NULL )
    {
        POM_clear_table( tbl_unneeded_to_uid );
    }
}

// Truncate with commit any transaction on Oracle. 
static void RUB_truncate_temp_tables()
{
    const char* storage = "";

    if (EIM_dbplat() == EIM_dbplat_oracle)
    {
        storage = " DROP STORAGE";
    }

    if (tbl_unneeded_from_uid != NULL)
    {
        char* sql = SM_sprintf("TRUNCATE TABLE %s%s", tbl_unneeded_from_uid, storage);
        RUB_dml_or_ddl(sql);
        SM_free(sql);
    }

    if (tbl_unneeded_to_uid != NULL)
    {
        char* sql = SM_sprintf("TRUNCATE TABLE %s%s", tbl_unneeded_to_uid, storage);
        RUB_dml_or_ddl(sql);
        SM_free(sql);
    }
}

static void RUB_drop_temp_tables()
{
     if ( tbl_unneeded_from_uid != NULL )
     {
         POM_add_table_name_to_session_drop_table_list( POM_TEMPORARY_TABLE, tbl_unneeded_from_uid );
         tbl_unneeded_from_uid = NULL;
     }

     if ( tbl_unneeded_to_uid != NULL )
     {
         POM_add_table_name_to_session_drop_table_list( POM_TEMPORARY_TABLE, tbl_unneeded_to_uid );
         tbl_unneeded_to_uid = NULL;
     }
 }

static void RUB_log_bkptrs_to_remove( int* count )
{
    *count = -1;

    if( args->min_flag)
    {
        return;          // We want to log minimum amount of data.
    }

    char* sql = NULL;

    if ( !args->alt )
    {
        sql = SM_sprintf( "SELECT from_uid, from_class, to_uid, to_class, bp_count FROM POM_BACKPOINTER WHERE from_uid in (SELECT from_uid FROM %s)", tbl_unneeded_from_uid );

    }
    else
    {
        sql = SM_sprintf( "SELECT from_uid, from_class, to_uid, to_class, bp_count FROM POM_BACKPOINTER WHERE to_uid IN (SELECT to_uid FROM %s)", tbl_unneeded_to_uid );
    }

    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    EIM_select_var_t vars[5];
    EIM_select_col( &( vars[0] ), EIM_puid,    "from_uid",   EIM_uid_length + 1, false );
    EIM_select_col( &( vars[1] ), EIM_integer, "from_class", sizeof( int ),      false );
    EIM_select_col( &( vars[2] ), EIM_puid,    "to_uid",     EIM_uid_length + 1, false );
    EIM_select_col( &( vars[3] ), EIM_integer, "to_class",   sizeof( int ),      false );
    EIM_select_col( &( vars[4] ), EIM_integer, "bp_count",   sizeof( int ),      false );
    int op_fail = EIM_exec_sql_bind( sql, &headers, &report, "RUB_log_bkptrs_to_remove() - Unable to read backpointers", 5, vars, 0, NULL );
    
    int row_cnt = -1;

    if ( op_fail == OK)
    {
        *count = 0;
    }

    if ( report != NULL && op_fail == OK )
    {
        int tmp_row_cnt = 0;
        for ( row = report; row != NULL; row = row->next, tmp_row_cnt++ );
        *count = tmp_row_cnt;

        row_cnt = 0;

        if ( args->verbose_flag )
        {
            cons_out( "Record,From_Uid,From_Class,To_Uid,To_class,Bp_Count," );
        }
        else
        {
            lprintf( "Log: Record,From_Uid,From_Class,To_Uid,To_class,Bp_Count,\n" );
        }

        for ( row = report; row != NULL; row = row->next, row_cnt++ )
        {
            std::stringstream data;
            data << (row_cnt + 1) << ",";

            int* int_ptr = NULL;
            char* char_ptr = NULL;

            // From uid
            char_ptr = NULL;
            EIM_find_value( headers, row->line, "from_uid", EIM_puid, &char_ptr );

            if ( char_ptr != NULL )
            {
                data << char_ptr;
            }

            data << ",";

            // From class
            int_ptr = NULL;
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &int_ptr );

            if ( int_ptr != NULL )
            {
                data << *int_ptr;
            }

            data << ",";

            // To uid
            char_ptr = NULL;
            EIM_find_value( headers, row->line, "to_uid", EIM_puid, &char_ptr );

            if ( char_ptr != NULL )
            {
                data << char_ptr;
            }

            data << ",";

            // To class
            int_ptr = NULL;
            EIM_find_value( headers, row->line, "to_class", EIM_integer, &int_ptr );

            if ( int_ptr != NULL )
            {
                data << *int_ptr;
            }

            data << ",";

            // bp count
            int_ptr = NULL;
            EIM_find_value( headers, row->line, "bp_count", EIM_integer, &int_ptr );

            if ( int_ptr != NULL )
            {
                data << *int_ptr;
            }

            data << ",";

            if ( args->verbose_flag )
            {
                cons_out( data.str() );
            }
            else
            {
                lprintf( "Log: %s\n", data.str().c_str() );
            }
        }
    }

    SM_free( sql );
    EIM_free_result( headers, report );
}

/*
** Remove any unneeded backpointers from the backpointer table. 
** -remove_unneeded_bp 
*/
static int remove_unneeded_bp_op( int* found_count )
{
    *found_count = -1;
    int ifail = OK;

    if ( args->uid_flag )
    {
        std::vector<std::string>* uid_vec_lcl = args->uid_vec;
        int uid_cnt = ( *uid_vec_lcl ).size();

        if ( uid_cnt > 100 )
        {
            ifail = POM_invalid_value;
            std::stringstream msg;
            msg << "\nError " << ifail << " A maximum of 100 UIDs can be specified.";
            cons_out( msg.str() );
        }
    }

    if ( ifail != OK )
    {
        return ifail;
    }

    // Initialize temporary tables.
    ifail = RUB_create_temporary_tables(args->alt);

    if ( ifail != OK )
    {
        return ifail;
    }

    RUB_clear_temp_tables();
    RUB_create_temporary_table_indexes();

    // Get class hierarchy metadata
    std::vector<hier_t> hier;
    getHierarchy( hier );

    // Get typed and untyped reference metadata + flattened class metadata
    std::vector<cls_t> meta;
    getRefMeta( meta, hier );

    // Filter out flattened class metadata.
    std::vector<minny_meta_t> flat_classes;
    filterOutFlatClasses( meta, flat_classes );

    if ( args->debug_flag )
    {
        dumpHierarchy( hier );
        dumpRefMetadata( meta );
    }

    // Keep track of how much data we are working with.
    // Optimization to stop checking flattened classes when count drops to zero.
    int from_uid_count = -1;
    int to_uid_count = -1; 
    int delta_stub_count = -1;

    // Does POM_object support revisioning?
    logical isPoVer = isVersionable( meta, "POM_object" );

    //
    // Identify uids from POM_BACKPOINTER.from_uid that don't exist.  If an object does not exist then the backpointer is not needed.
    //
    if ( !args->alt )
    {
        char* where = create_uid_specific_where_clause( " AND a.from_uid", args->uid_vec );
        char* sql = SM_sprintf( "INSERT INTO %s (from_uid) (SELECT DISTINCT from_uid FROM POM_BACKPOINTER a LEFT JOIN PPOM_OBJECT b ON a.from_uid = b.puid WHERE b.puid is NULL%s)", tbl_unneeded_from_uid, ( where ? where : "" ) );
        from_uid_count = RUB_dml_or_ddl( sql );
        SM_free( sql );
        SM_free( where );

        // Identify all the classes found in the POM_BACKPOINTER.from_class.
        std::vector<minny_meta_t> flat_classes_with_from_bps;
        logical unknown_class = RUB_get_flat_classes_with_bps( flat_classes, tbl_unneeded_from_uid, from_uid_col_name, flat_classes_with_from_bps );

        if ( !args->force_flag && !unknown_class )
        {
            logger()->printf( "remove_unneeded_bp_op(): Processing %d from_uids via optimized processing path.\n", from_uid_count );

            if ( from_uid_count > 0 )
            {
                for ( int i = 0; i < flat_classes_with_from_bps.size(); i++ )
                {
                    char* sql = SM_sprintf( "DELETE FROM %s WHERE from_uid IN (SELECT puid FROM %s)", tbl_unneeded_from_uid, flat_classes_with_from_bps[i].db_name );
                    from_uid_count -= RUB_dml_or_ddl( sql );
                    SM_free( sql );

                    if ( from_uid_count < 1 )
                    {
                        from_uid_count = get_row_count( tbl_unneeded_from_uid );

                        if ( from_uid_count < 1 )
                        {
                            logger()->printf( "remove_unneeded_bp_op(): the number of from_uid target puids has gone to zero. NOT looking for additional puids in any additional flattened classes.\n" );
                            break;
                        }
                    }
                }
            }
        }
        else
        {
            logger()->printf( "remove_unneeded_bp_op(): Processing %d from_uids via forced processing path.\n", from_uid_count );
            
            for ( int i = 0; i < flat_classes.size(); i++ )
            {
                char* sql = SM_sprintf( "DELETE FROM %s WHERE from_uid IN (SELECT puid FROM %s)", tbl_unneeded_from_uid, flat_classes[i].db_name );
                from_uid_count -= RUB_dml_or_ddl( sql );
                SM_free( sql );
            }
        }
    }


    //
    // Identify uids from POM_BACKPOINTER.to_uid that don't exist.  If an object does not exist, and is not stubbed, then the backpointer is not needed.
    //
    if ( args->alt )
    {
        char* where = create_uid_specific_where_clause( " AND a.to_uid", args->uid_vec );
        char* sql = SM_sprintf( "INSERT INTO %s (to_uid) (SELECT DISTINCT to_uid FROM POM_BACKPOINTER a LEFT JOIN PPOM_OBJECT b ON a.to_uid = b.%s WHERE b.%s is NULL%s)",
            tbl_unneeded_to_uid, ( isPoVer ? aoid_col_name : puid_col_name ), ( isPoVer ? aoid_col_name : puid_col_name ), ( where ? where : "" ) );
        to_uid_count = RUB_dml_or_ddl( sql );
        SM_free( sql );
        SM_free( where );

        sql = SM_sprintf( "DELETE FROM %s WHERE to_uid IN (SELECT pobject_uid FROM PPOM_STUB)", tbl_unneeded_to_uid );
        delta_stub_count = RUB_dml_or_ddl( sql );
        to_uid_count -= delta_stub_count;
        SM_free( sql );

        // Identify all the classes found in the POM_BACKPOINTER.to_class.
        std::vector<minny_meta_t> flat_classes_with_to_bps;
        logical unknown_class = RUB_get_flat_classes_with_bps( flat_classes, tbl_unneeded_to_uid, to_uid_col_name, flat_classes_with_to_bps );

        if ( !args->force_flag && !unknown_class )
        {
            logger()->printf( "remove_unneeded_bp_op(): Processing %d to_uids via optimized processing path.\n", to_uid_count );
                                
            if ( to_uid_count > 0 )
            {
                for ( int i = 0; i < flat_classes_with_to_bps.size(); i++ )
                {
                    if ( isReferenceable( &meta[flat_classes_with_to_bps[i].cls_pos] ) )
                    {
                        logical isVer = isVersionable( &meta[flat_classes_with_to_bps[i].cls_pos] );
                        char* sql = SM_sprintf( "DELETE FROM %s WHERE to_uid IN (SELECT %s FROM %s)", tbl_unneeded_to_uid, ( isVer ? aoid_col_name : puid_col_name ), flat_classes_with_to_bps[i].db_name );
                        to_uid_count -= RUB_dml_or_ddl( sql );
                        SM_free( sql );

                        if ( to_uid_count < 1 )
                        {
                            to_uid_count = get_row_count( tbl_unneeded_to_uid );

                            if ( to_uid_count < 1 )
                            {
                                logger()->printf( "remove_unneeded_bp_op(): the number of to_uid target puids has gone to zero. NOT looking for additional puids in any additional flattened classes.\n" );
                                break;
                            }
                        }
                    }
                }
            }
        }
        else
        {
            logger()->printf( "remove_unneeded_bp_op(): Processing %d to_uids via forced processing path.\n", to_uid_count );

            for ( int i = 0; i < flat_classes.size(); i++ )
            {
                logical isVer = isVersionable( &meta[flat_classes[i].cls_pos] );
                char* sql = SM_sprintf( "DELETE FROM %s WHERE to_uid IN (SELECT puid FROM %s)", tbl_unneeded_to_uid, ( isVer ? aoid_col_name : puid_col_name ), flat_classes[i].db_name );
                RUB_dml_or_ddl( sql );
                SM_free( sql );
            }
        }
    }

    cons_out( "" ); 

    // Log internal optimization counts
    logger()->printf( "from_uid_count = %d\n", from_uid_count );
    logger()->printf( "to_uid_count = %d\n", to_uid_count );
    logger()->printf( "delta_stub_count = %d\n", delta_stub_count );
    
    int bkptr_removal_cnt = -1;
    RUB_log_bkptrs_to_remove( &bkptr_removal_cnt );

    if ( bkptr_removal_cnt < 0 )  // If not logging backpointers to be deleted for performance reasons then just use the found UID counts.
    {
        bkptr_removal_cnt = 0; 

        if( from_uid_count > 0)
        {
            bkptr_removal_cnt += from_uid_count;   // Add the from_uid_count if working on from_uids.      
        }

        if ( to_uid_count > 0 )
        {
            bkptr_removal_cnt += to_uid_count;    // Add the to_uid_count if working on the to_uids.
        }    
    }
    
    *found_count = bkptr_removal_cnt;

    if ( !args->commit_flag )
    {   
        std::stringstream msg;

        if ( !args->verbose_flag && !args->min_flag )
        {
            msg << "\nAdd the -v option to dump the backpointers to the console.";
        }

        msg << "\nAdd the -commit option to permanently delete " << bkptr_removal_cnt << " unneeded backpointers.";
        cons_out( msg.str() );
    }
    else
    {
        if ( bkptr_removal_cnt > 0 )
        {
            int delete_count = -1;
            EIM_commit_transaction( "" );
            START_WORKING_TX( remove_unneeded_bp_tx, "remove_unneeded_bp_tx" );

            ERROR_PROTECT

            char* sql = NULL;

            if ( !args->alt )
            {
                sql = SM_sprintf( "DELETE FROM POM_BACKPOINTER WHERE from_uid IN (SELECT from_uid FROM %s)", tbl_unneeded_from_uid ); 
            }
            else
            {
                sql = SM_sprintf( "DELETE FROM POM_BACKPOINTER WHERE to_uid IN (SELECT to_uid FROM %s)", tbl_unneeded_to_uid ); 
            }
            delete_count = RUB_dml_or_ddl( sql );
            SM_free( sql );

            ERROR_RECOVER

            if ( ifail == OK )
            {
                ifail = ERROR_ask_failure_code();

                if( !ifail )
                {
                    ifail = POM_internal_error;              
                }
            }
            ERROR_END

            if( ifail )
            {
                ROLLBACK_WORKING_TX( remove_unneeded_bp_tx, "remove_unneeded_bp_tx" );
                std::stringstream msg;
                msg << "Deletion of unneeded backpointers has been rolled back. (ifail=" << ifail << ") See syslog for details.";
                cons_out( msg.str() );
            }
            else
            {
                // ROLLBACK_WORKING_TX( remove_unneeded_bp_tx, "remove_unneeded_bp_tx" );
                COMMIT_WORKING_TX( remove_unneeded_bp_tx, "remove_unneeded_bp_tx" );
                std::stringstream msg;
                msg << "Deletion of " << delete_count << " unneeded backpointers has been successfully committed.";
                cons_out( msg.str());
            }
         
            EIM_start_transaction();
        }
        else
        {
            cons_out( "No unneeded backpointers were found.");      
        }
    }
    
    // Clear temporary tables so they can be dropped.
    // On oracle this commits any open transactions. 
    // The work was done above using a Utility Transaction.  
    RUB_truncate_temp_tables();

    // Remove indexes from temporary tables.
    RUB_drop_temporary_table_indexes();

    if( !args->ignore_errors_flag)
    {
        // Interesting that tables can't be dropped if they contain data.
        RUB_drop_temp_tables();  
    }
    
    return ifail;
}
/* ********************************************************************************
** END OF: remove_unneeded_bp_op() routines. 
** *******************************************************************************/

#define MAX_DATA_LINE_LEN 2050
/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* unlock_and_unload                                             */
static int unlock_and_unload( tag_t obj_tag )
{
    int ifail = POM_refresh_instances( 1, &obj_tag, NULLTAG, POM_no_lock );

    if ( !ifail )
    {
        ifail = POM_unload_instances( 1, &obj_tag );
    }

    return ifail;
}

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* Convert a list of strings to CSV format                       */
static std::string list_to_csv_string( std::vector<std::string> list )
{
    std::stringstream sb;

    for ( std::string list_item : list )
    {
        bool quote = false;
        int comma_pos = list_item.find( ',' );
        int quote_pos = list_item.find( '"' );

        if ( comma_pos < list_item.length( ) || quote_pos < list_item.length( ) )
        {
            quote = true;
        }

        if ( quote )
        {
            std::vector<std::string> strings;
            std::istringstream strstm( list_item );
            std::string s;
            while ( getline( strstm, s, '"' ) ) {
                strings.push_back( s );
            }

            // Add leading quote.
            sb << '"';

            for ( int k = 0; k < strings.size( ); k++ )
            {
                sb << strings[k];

                if ( k < strings.size( ) - 1 )
                {
                    sb << "\"\"";
                }
                else if ( list_item.length( ) > 0 && list_item[(list_item.length( ) - 1)] == '"' )
                {
                    sb << "\"\"";
                }
            }

            // Add trailing quote.
            sb << '"';
        }
        else
        {
            sb << list_item;
        }
        sb <<  ',';
    }
    return sb.str( );
}

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* Find CSV parameter starting location                          */
static char* find_csv_field( char* start )
{
    char* ret = NULL;
    char* cp = start;

    if ( cp == NULL )
    {
        return(ret);
    }

    // skip leading whitespace.
    while ( isspace( *cp ) )
    {
        cp++;
    }

    if ( *cp != ',' || *cp != '\0' )
    {
        ret = cp;
    }

    return(ret);
}

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* Move past the current CSV field to the begining of the next CSV field.*/
static char* move_past_csv_field( char* cp )
{
    while ( isspace( *cp ) )
    {
        cp++;
    }

    if ( *cp == '\0' )
    {
        return cp;
    }

    if ( *cp == ',' )
    {
        cp++;
        return cp;
    }

    ERROR_raise( ERROR_line, POM_invalid_value, "Invalid data after the end of the CSV and before the next field (%s)", cp );

    return cp;
}

static int find_csv_field_length( char* start )
{
    int len = 0;
    char* cp = start;
    logical in_quote = false;

    if ( !start )
    {
        return len;
    }

    if ( *cp == '"' )
    {
        in_quote = true;
        cp++;
        len++;
    }

    if ( in_quote )
    {
        // Quote mode - terminate on single quote.
        while ( *cp != '\0' )
        {
            if ( *cp == '"' )
            {
                len++;
                cp++;

                if ( *cp != '\0' )
                {
                    if ( *cp != '"' )
                    {
                        // Terminate on single quote.
                        break;
                    }

                    len++;
                    cp++;
                }
            }
            else
            {
                if ( *cp != '\0' )
                {
                    len++;
                    cp++;
                }
            }
        }
    }
    else
    {
        // Normal mode - terminate on comma (,).
        while ( *cp != '\0' && *cp != ',' )
        {
            if ( *cp != '\0' )
            {
                len++;
                cp++;
            }
        }
    }
    return len;
}

static std::string extract_csv_data( char* start )
{
    std::string ret = "";

    if ( !start )
    {
        return ret;
    }

    char dst[MAX_DATA_LINE_LEN + 1];
    char* buf = dst;
    *buf = '\0';
    char* cp = start;
    logical in_quote = false;

    if ( *cp == '"' )
    {
        in_quote = true;
        cp++;
    }

    if ( in_quote )
    {
        // Quote mode - terminate on single quote.
        while ( *cp != '\0' )
        {
            if ( *cp == '"' )
            {
                cp++;

                if ( *cp != '\0' )
                {
                    if ( *cp != '"' )
                    {
                        // Terminate on single quote.
                        break;
                    }

                    // Convert double quote into a single quote.
                    *buf++ = *cp++;
                    *buf = '\0';
                }
            }
            else
            {
                if ( *cp != '\0' )
                {
                    *buf++ = *cp++;
                    *buf = '\0';
                }
            }
        }
    }
    else
    {
        // Normal mode - terminate on comma (,).
        while ( *cp != '\0' && *cp != ',' )
        {
            if ( *cp != '\0' )
            {
                *buf++ = *cp++;
                *buf = '\0';
            }
        }
    }

    ret = dst;
    return ret;
}

static void read_next_csv_line( FILE* fp, std::vector<std::string> &field_data )
{
    char line[MAX_DATA_LINE_LEN + 1];
    field_data.clear( );

    if ( fnd_fgets( line, MAX_DATA_LINE_LEN, fp ) )
    {
        line[MAX_DATA_LINE_LEN] = '\0';

        int siz = strlen( line );

        for ( int i = siz - 1; i >= 0; i-- )
        {
            int ch = (int)line[i];

            if ( ch != !0x0d && ch != 0x0a )
            {
                break;
            }

            line[i] = '\0';
        }

        // Look for beginning of first field.
        char* cp = find_csv_field( &(line[0]) );

        if ( cp != NULL && *cp != '\0' )
        {
            // Extract data from this field then move to next field.
            while ( cp != NULL && *cp != '\0' )
            {
                int len = find_csv_field_length( cp );
                field_data.push_back( extract_csv_data( cp ));

                // Find begining of next field.
                cp += len;
                cp = move_past_csv_field( cp );
                cp = find_csv_field( cp );
            }
        }
    }
}


static bool EV_output_fail( int line_no, std::vector<std::string>& vla_mods, std::string error_message )
{
    std::stringstream sb;
    sb << "line " << line_no << " - ERROR: ";
    sb << list_to_csv_string( vla_mods );
    sb << "ERROR: " << error_message;
    cons_out( sb.str( ) );
    return false;
}

static void EV_output_success( int line_no, std::vector<std::string>& vla_mods)
{
    std::stringstream sb;
    sb << "line " << line_no << " - SUCCESS: ";
    sb << list_to_csv_string( vla_mods );
    sb << "SUCCESS";
    cons_out( sb.str( ) );
}

static bool edit_vlas( int line_no, std::vector<std::string>& header, std::vector<std::string>& vla_mods )
{
    bool success = false;
    int ifail = POM_ok;

    // Step 1. 
    // Validate the action
    // Convert UID to object tag 
    // Cache cpid of object tag
    // Load and lock object tag.

    std::string  act = vla_mods[0];      // edit array action.
    std::string  cls = vla_mods[1];      // class of object to edit
    std::string  att = "";               // VLA attribute.
    std::string  uid = vla_mods[2];      // UID of object to edit
    std::string  pos = vla_mods[3];      // position of vlas to edit.
    int vla_pos      = -1;               // Numeric position within VLA.

    tag_t        obj_tag = NULLTAG;      // Object being modified
    tag_t        cls_tag = NULLTAG;      // Class of object being modified
    tag_t        att_tag = NULLTAG;      // Attribute being modified.
    unsigned int cache_cnt = 0;

    if ( act.compare( "insert" ) != 0 && act.compare( "delete" ) != 0 && act.compare( "append" ) != 0 && act.compare( "update" ) != 0 )
    {
        if ( ifail != OK || cache_cnt < 1 )
        {
            std::stringstream msg;
            msg << "An invalid action (" << act << ") was specified. Action needs to be insert, delete, append or update";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            return success;
        }
    }

    vla_pos = std::stoi( pos );

    if ( vla_pos < 0 )
    {
        std::stringstream msg;
        msg << "An invalid VLA position (" << vla_pos << ") was specified.";
        EV_output_fail( line_no, vla_mods, msg.str( ) );
        return success;
    }

    ifail = POM_string_to_tag( uid.c_str( ), &obj_tag );

    if ( ifail != OK )
    {
        std::stringstream msg;
        msg << "Unable to convert the uid (" << uid << ") into a tag. Error = " << ifail << ".";
        EV_output_fail( line_no, vla_mods, msg.str( ) );
        return success;
    }

    ifail = POM_class_id_of_class ( cls.c_str( ), &cls_tag );

    if ( ifail != OK )
    {
        std::stringstream msg;
        msg << "Unable to identify class " << cls << ". Error = " << ifail << ".";
        EV_output_fail( line_no, vla_mods, msg.str( ) );
        return success;
    }

    ifail = DMS_cache_cpids_of_class( 1, &obj_tag, cls.c_str( ), NULL, &cache_cnt );

    if ( ifail != OK || cache_cnt < 1 )
    {
        std::stringstream msg;
        msg << "Unable to cache cpid of " << uid << ". I.e. UID was not found in class " << cls << ". Error = " << ifail << ". cache_cnt = " << cache_cnt << ".";
        EV_output_fail( line_no, vla_mods, msg.str( ) );
        return success;
    }

    ifail = POM_load_instances( 1, &obj_tag, NULLTAG, POM_modify_lock );

    if ( ifail != OK )
    {
        std::stringstream msg;
        msg << "Unable to load and lock " << uid << ". Error = " << ifail << ".";
        EV_output_fail( line_no, vla_mods, msg.str( ) );
        return success;
    }

    // Step 2. 
    // Make the VLA edit. 
    success = true;

    for ( int i = 4; i < header.size( ); i++ )
    {
        //
        // Identify and prepare target VLA attribute 
        //
        att = header[i];

        ifail = POM_attr_id_of_attr ( att.c_str( ), cls.c_str( ), &att_tag );

        if ( ifail != OK )
        {
            unlock_and_unload( obj_tag );
            std::stringstream msg;
            msg << "Unable to identify attribute " << att << " in class " << cls << ". Error = " << ifail << ".";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            return false;
        }

        char** att_names = NULL;
        int* types = NULL;
        int* str_lens = NULL;
        tag_t* ref_class = NULL;
        int* lengths = NULL;
        int* descs = NULL;
        int* fails = NULL;

        ifail = POM_describe_attrs( cls_tag, 1, &att_tag, &att_names, &types, &str_lens, &ref_class, &lengths, &descs, &fails );

        int att_type = (types ? *types : 0);
        int att_length = (lengths ? *lengths : 0);

        SM_free( (void*)att_names );
        SM_free( (void*)types );
        SM_free( (void*)str_lens );
        SM_free( (void*)ref_class );
        SM_free( (void*)lengths );
        SM_free( (void*)descs );
        SM_free( (void*)fails );

        if ( ifail != OK )
        {
            unlock_and_unload( obj_tag );
            std::stringstream msg;
            msg << "Unable to describe attribute " << att << " from class " << cls << ". Error = " << ifail << ".";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            return false;
        }

        if ( att_length != -1 )
        {
            unlock_and_unload( obj_tag );
            std::stringstream msg;
            msg << "Attribute " << att << " from class " << cls << " is NOT a VLA (length = " << att_length << ").";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            return false;
        }

        if ( att_type != POM_int && att_type != POM_string && att_type != POM_typed_reference && att_type != POM_untyped_reference )
        {
            unlock_and_unload( obj_tag );
            std::stringstream msg;
            msg << "Attribute " << att << " from class " << cls << " is NOT a POM_int, POM_string, POM_typed_reference or POM_untyped_reference (type = " << att_type << ").";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            return false;
        }

        // 
        // Prepare target data.
        //
        char* new_string = NULL;
        int   new_int = 0;
        tag_t new_ref = NULLTAG;
        bool  prep_fail = false;

        if ( act.compare ( "delete" ) != 0 )
        {
            switch ( att_type )
            {
            case POM_string:
                new_string = SM_string_copy( vla_mods[i].c_str( ) );
                break;
            case POM_int:
                new_int = std::stoi( vla_mods[i] );
                break;
            case POM_typed_reference:
            case POM_untyped_reference:
                new_string = SM_string_copy( vla_mods[i].c_str( ) );
                char* ptr = strchr( new_string, ':' );

                // Null terminate UID before class
                if ( ptr != NULL )
                {
                    *ptr = '\0';
                    ptr++;
                }

                ifail = POM_string_to_tag( new_string, &new_ref );

                if ( ifail != OK )
                {
                    std::stringstream msg;
                    msg << "Unable to convert the target uid (" << new_string << ") into a tag. Error = " << ifail << ".";
                    EV_output_fail( line_no, vla_mods, msg.str( ) );
                    prep_fail = true;
                    success = false;
                }
                else
                {
                    unsigned int cnt = 0;
                    if ( !ptr || strcmp( ptr, "skip_cache" ) != 0 )
                    {
                        // Cache CPID of referenced object.
                        if ( ptr == NULL )
                        {
                            // Look in the ppom_object class. 
                            ifail = DMS_cache_cpids_of_class( 1, &new_ref, "POM_object", NULL, &cnt );
                        }
                        else
                        {
                            // Look in the specified class.
                            ifail = DMS_cache_cpids_of_class( 1, &new_ref, ptr, NULL, &cnt );
                        }

                        if ( ifail != OK || cnt < 1 )
                        {
                            std::stringstream msg;
                            msg << "Unable to cache cpid of the new reference " << new_string << ". Error = " << ifail << ". cache_cnt = " << cnt << ".";
                            EV_output_fail( line_no, vla_mods, msg.str( ) );
                            prep_fail = true;
                            success = false;
                        }
                    }
                }
                break;
            }
        }

        //
        // Perform VLA operation.
        //
        if ( !prep_fail )
        {
            if ( act.compare( "insert" ) == 0 )
            {
                switch ( att_type )
                {
                case POM_string:
                    ifail = POM_insert_attr_strings( 1, &obj_tag, att_tag, vla_pos, 1, &new_string );
                    break;
                case POM_int:
                    ifail = POM_insert_attr_ints( 1, &obj_tag, att_tag, vla_pos, 1, &new_int );
                    break;
                case POM_typed_reference:
                case POM_untyped_reference:
                    ifail = POM_insert_attr_tags( 1, &obj_tag, att_tag, vla_pos, 1, &new_ref );
                    break;
                }
            }
            else if ( act.compare( "delete" ) == 0 )
            {
                ifail = POM_remove_from_attr( 1, &obj_tag, att_tag, vla_pos, 1 );
            }
            else if ( act.compare( "append" ) == 0 )
            {
                switch ( att_type )
                {
                case POM_string:
                    ifail = POM_append_attr_strings( 1, &obj_tag, att_tag, 1, &new_string );
                    break;
                case POM_int:
                    ifail = POM_append_attr_ints( 1, &obj_tag, att_tag, 1, &new_int );
                    break;
                case POM_typed_reference:
                case POM_untyped_reference:
                    ifail = POM_append_attr_tags( 1, &obj_tag, att_tag, 1, &new_ref );
                    break;
                }      
            }
            else if ( act.compare( "update" ) == 0 )
            {
                switch ( att_type )
                {
                case POM_string:
                    ifail = POM_set_attr_strings( 1, &obj_tag, att_tag, vla_pos, 1, &new_string );
                    break;
                case POM_int:
                    ifail = POM_set_attr_ints( 1, &obj_tag, att_tag, vla_pos, 1, &new_int );
                    break;
                case POM_typed_reference:
                case POM_untyped_reference:
                    ifail = POM_set_attr_tags( 1, &obj_tag, att_tag, vla_pos, 1, &new_ref );
                    break;
                }
            }

            if ( ifail )
            {
                std::stringstream msg;
                msg << "Unable to " << act << " VLA record (pos = " << vla_pos << ") from " << cls << ":" << att << ":" << uid << ".  Error = " << ifail << ".";
                EV_output_fail( line_no, vla_mods, msg.str( ) );
                success = false;
            }
        }

        SM_free( (void*)new_string );
    }

    if ( success )
    {
        int ifail = POM_save_instances( 1, &obj_tag, false );

        if ( ifail )
        {
            std::stringstream msg;
            msg << "Unable to save object " << uid << ". Error = " << ifail << ".";
            EV_output_fail( line_no, vla_mods, msg.str( ) );
            success = false;
            unlock_and_unload( obj_tag );
        }
        else
        {
            EV_output_success( line_no, vla_mods );
            unlock_and_unload( obj_tag );
        }
    }
    else
    {
        unlock_and_unload( obj_tag );
    }
    return success;
}

static int edit_array_op( )
{
    cons_out( "" );
    int ifail = OK;

    // Check for license if doing -commit
    if ( args->commit_flag )
    {
        ifail = licenseFor( "-edit_array" );

        if ( ifail != OK )
        {
            return(ifail);
        }
    }

    if ( !args->file_name )
    {
        std::stringstream msg;
        msg << "ERROR: -f=<file_name> option is required for \"-edit_array\" functionality.";
        cons_out( msg.str( ) );
        return POM_invalid_value;
    }

    std::vector<unsigned char> inString;
    FILE* fp = fnd_fopen( args->file_name, "r" );

    if ( 0 == fp )
    {
        std::stringstream msg;
        msg << "ERROR: Unable to open file";
        cons_out( msg.str( ) );
        return POM_invalid_value;
    }

    // 
    // Read CSV header line. 
    //
    std::vector<std::string> header;
    read_next_csv_line( fp, header );

    if ( header.size() < 5 )
    {
        cons_out( "ERROR: Insufficient number of header columns found in CSV input file. (action,class,uid,postion,<vla_attr>,...)" );
        fclose( fp );
        return POM_invalid_value;
    }

    // Header: action,class,uid,vla_position,<vla_attr1>,<vla_attr2>,.....,<vla_attrn> 
    ifail = POM_ok;
    if ( strcmp( header[0].c_str(), "action" ) != 0 )
    {
        cons_out( "ERROR: Invalid or missing \"action\" header in the first position." );
        ifail = POM_invalid_value;
    }

    if ( strcmp( header[1].c_str( ), "class" ) != 0 )
    {
        cons_out( "ERROR: Invalid or missing \"class\" header in the second position." );
        ifail = POM_invalid_value;
    }

    if ( strcmp( header[2].c_str( ), "uid" ) != 0 )
    {
        cons_out( "ERROR: Invalid or missing \"uid\" header in the third position." );
        ifail = POM_invalid_value;
    }

    if ( strcmp( header[3].c_str( ), "pos" ) != 0 )
    {
        cons_out( "ERROR: Invalid or missing \"position\" header in the forth position." );
        ifail = POM_invalid_value;
    }

    if ( ifail )
    {
        fclose( fp );
        return ifail;
    }

    // 
    // Read in all data from CSV file.
    //
    std::vector<std::vector<std::string>> file_data;

    int cnt = 0;

    do
    {
        std::vector<std::string>  data;
        read_next_csv_line( fp, data );

        cnt = data.size( );

        if ( cnt > 0 )
        {
            if ( cnt != header.size( ) )
            {
                cons_out( "ERROR: There must be the same number of data columns as there are header columns. Check your CSV input file." );
                fclose( fp );
                ifail = POM_invalid_value;
                return ifail;
            }

            file_data.push_back( data );
        }

    } while ( cnt > 0 );

    fclose( fp );

    // 
    // Edit VLAs.
    //
    int line_no = 0;
    bool success = true;

    START_WORKING_TX( edit_vlas_tx, "edit_vlas_tx" );

    ERROR_PROTECT

    for ( auto data : file_data )
    {
        line_no++;

        if( !edit_vlas( line_no, header, data ))
        {
            success = false;
        }
    }

    ERROR_RECOVER

    ifail = ERROR_ask_failure_code( );
    EIM_clear_error( );
    success = false;

    ERROR_END

    if ( !success || ifail )
    {
        ROLLBACK_WORKING_TX( edit_vlas_tx, "edit_vlas_tx" );
        std::stringstream msg;
        msg << "Failure detect. CSV file line number = " << line_no << ", ifail = " << ifail << ", success = " << (success ? "true" : "false") << ".";
        cons_out( msg.str( ) );
        if ( !ifail )
        {
            ifail = POM_invalid_value;
        }
    }
    else if( !args->commit_flag )
    {
        ROLLBACK_WORKING_TX( edit_vlas_tx, "edit_vlas_tx" );
        std::stringstream msg;
        msg << "-commit option NOT specified, rolling back transaction.";
        cons_out( msg.str( ) );    
    }
    else
    {
        COMMIT_WORKING_TX( edit_vlas_tx, "edit_vlas_tx" );
        cons_out( "Processing complete and VLA edits have been committed." );
    }

    return ifail;
}

/* ********************************************************************************
** START OF: validate_bp2_op() routines.
** *******************************************************************************/
/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Similar to fmt::format(), however this will backport to prior releases.
*/
static std::string fmt__format( const char* format, ... )
{
    char* res;
    va_list ap;

    va_start( ap, format );
    res = SM_vsprintf( format, ap );
    va_end( ap );

    std::string ret = res;
    SM_free( res );
    return ret;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Logs the backpointers with incorrect from-class values.
*/
static int log_bps_w_invalid_from_class( const char* sql, const char* message, int n_binds, EIM_bind_var_t* binds, int target_cpid, std::vector<std::string>& values )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    values.clear();
    long long count = 0;

    EIM_select_var_t select_var[5];
    EIM_select_col( &select_var[0], EIM_puid,    "from_uid", EIM_uid_length+1, false );
    EIM_select_col( &select_var[1], EIM_integer, "from_class", sizeof(int), false );
    EIM_select_col( &select_var[2], EIM_puid,    "to_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[3], EIM_integer, "to_class", sizeof( int ), false );
    EIM_select_col( &select_var[4], EIM_integer, "bp_count", sizeof( int ), false );
    EIM_exec_sql_bind( sql, &headers, &report, message, 5, select_var, n_binds, binds );


    if ( report != NULL )
    {
        logger()->printf( "BPV:FrmCls,from_uid,from_class,to_uid,to_class,bp_count,correct_from_class,comment,\n" );   

        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            count++;

            char* pfrom_uid = NULL;
            int*  pfrom_class = NULL;
            char* pto_uid = NULL;
            int*  pto_class = NULL;
            int*  pbp_count = NULL;
            EIM_find_value( headers, row->line, "from_uid",   EIM_puid, &pfrom_uid );
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &pfrom_class );
            EIM_find_value( headers, row->line, "to_uid",     EIM_puid, &pto_uid );
            EIM_find_value( headers, row->line, "to_class",   EIM_integer, &pto_class );
            EIM_find_value( headers, row->line, "bp_count",   EIM_integer, &pbp_count );

            logger()->printf( "BPV:FrmCls,%s,%d,%s,%d,%d,%d,,\n",
                ( pfrom_uid ? pfrom_uid : "" ),  ( pfrom_class ? *pfrom_class : -1 ),
                ( pto_uid ? pto_uid : "" ),      ( pto_class ? *pto_class : -1 ),
                ( pbp_count ? *pbp_count : -1 ), target_cpid );
        }
    }

    EIM_free_result( headers, report );
    values.push_back( std::to_string( count ) );
    return POM_ok;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Logs the backpointers with incorrect from-class values.
*/
static int log_bps_w_invalid_counts( const char* sql, const char* message, int n_binds, EIM_bind_var_t* binds, std::vector<std::string>& values )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    values.clear();
    long long count = 0;

    EIM_select_var_t select_var[6];
    EIM_select_col( &select_var[0], EIM_puid, "from_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[1], EIM_integer, "from_class", sizeof( int ), false );
    EIM_select_col( &select_var[2], EIM_puid, "to_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[3], EIM_integer, "to_class", sizeof( int ), false );
    EIM_select_col( &select_var[4], EIM_integer, "bp_count", sizeof( int ), false );
    EIM_select_col( &select_var[5], EIM_integer, "correct_count", sizeof( int ), false );
    EIM_exec_sql_bind( sql, &headers, &report, message, 6, select_var, n_binds, binds );

    if ( report != NULL )
    {
        logger()->printf( "BPV:InvCnt,from_uid,from_class,to_uid,to_class,bp_count,correct_count,comment,\n" );

        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            count++;

            char* pfrom_uid = NULL;
            int* pfrom_class = NULL;
            char* pto_uid = NULL;
            int* pto_class = NULL;
            int* pbp_count = NULL;
            int* pcor_count = NULL;
            EIM_find_value( headers, row->line, "from_uid", EIM_puid, &pfrom_uid );
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &pfrom_class );
            EIM_find_value( headers, row->line, "to_uid", EIM_puid, &pto_uid );
            EIM_find_value( headers, row->line, "to_class", EIM_integer, &pto_class );
            EIM_find_value( headers, row->line, "bp_count", EIM_integer, &pbp_count );
            EIM_find_value( headers, row->line, "correct_count", EIM_integer, &pcor_count );

            logger()->printf( "BPV:InvCnt,%s,%d,%s,%d,%d,%d,,\n",
                ( pfrom_uid ? pfrom_uid : "" ),  ( pfrom_class ? *pfrom_class : -1 ),
                ( pto_uid ? pto_uid : "" ),      ( pto_class ? *pto_class : -1 ),
                ( pbp_count ? *pbp_count : -1 ), ( pcor_count ? *pcor_count : -1 ) );
        }
    }

    EIM_free_result( headers, report );
    values.push_back( std::to_string( count ) );
    return POM_ok;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Logs missing backpointers.
*/
static int log_missing_bps( const char* sql, const char* message, int n_binds, EIM_bind_var_t* binds, std::vector<std::string>& values )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    values.clear();
    long long count = 0;

    EIM_select_var_t select_var[5];
    EIM_select_col( &select_var[0], EIM_puid, "from_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[1], EIM_integer, "from_class", sizeof( int ), false );
    EIM_select_col( &select_var[2], EIM_puid, "to_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[3], EIM_integer, "to_class", sizeof( int ), false );
    EIM_select_col( &select_var[4], EIM_integer, "bp_count", sizeof( int ), false );
    EIM_exec_sql_bind( sql, &headers, &report, message, 5, select_var, n_binds, binds );

    if ( report != NULL )
    {
        logger()->printf( "BPV:MisBP,from_uid,from_class,to_uid,to_class,bp_count,,comment,\n" );

        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            count++;

            char* pfrom_uid = NULL;
            int* pfrom_class = NULL;
            char* pto_uid = NULL;
            int* pto_class = NULL;
            int* pbp_count = NULL;
            EIM_find_value( headers, row->line, "from_uid", EIM_puid, &pfrom_uid );
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &pfrom_class );
            EIM_find_value( headers, row->line, "to_uid", EIM_puid, &pto_uid );
            EIM_find_value( headers, row->line, "to_class", EIM_integer, &pto_class );
            EIM_find_value( headers, row->line, "bp_count", EIM_integer, &pbp_count );

            logger()->printf( "BPV:MisBP,%s,%d,%s,%d,%d,,,\n",
                ( pfrom_uid ? pfrom_uid : "" ),  ( pfrom_class ? *pfrom_class : -1 ),
                ( pto_uid ? pto_uid : "" ),      ( pto_class ? *pto_class : -1 ),
                ( pbp_count ? *pbp_count : -1 ) );
        }
    }

    EIM_free_result( headers, report );
    values.push_back( std::to_string( count ) );
    return POM_ok;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Logs unneeded backpointers.
*/
static int log_unneeded_bps( const char* sql, const char* message, int n_binds, EIM_bind_var_t* binds, std::vector<std::string>& values )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    values.clear();
    long long count = 0;

    EIM_select_var_t select_var[5];
    EIM_select_col( &select_var[0], EIM_puid, "from_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[1], EIM_integer, "from_class", sizeof( int ), false );
    EIM_select_col( &select_var[2], EIM_puid, "to_uid", EIM_uid_length + 1, false );
    EIM_select_col( &select_var[3], EIM_integer, "to_class", sizeof( int ), false );
    EIM_select_col( &select_var[4], EIM_integer, "bp_count", sizeof( int ), false );
    EIM_exec_sql_bind( sql, &headers, &report, message, 5, select_var, n_binds, binds );

    if ( report != NULL )
    {
        logger()->printf( "BPV:UnNBP,from_uid,from_class,to_uid,to_class,bp_count,,comment,\n" );

        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            count++;

            char* pfrom_uid = NULL;
            int* pfrom_class = NULL;
            char* pto_uid = NULL;
            int* pto_class = NULL;
            int* pbp_count = NULL;
            EIM_find_value( headers, row->line, "from_uid", EIM_puid, &pfrom_uid );
            EIM_find_value( headers, row->line, "from_class", EIM_integer, &pfrom_class );
            EIM_find_value( headers, row->line, "to_uid", EIM_puid, &pto_uid );
            EIM_find_value( headers, row->line, "to_class", EIM_integer, &pto_class );
            EIM_find_value( headers, row->line, "bp_count", EIM_integer, &pbp_count );

            logger()->printf( "BPV:UnNBP,%s,%d,%s,%d,%d,,,\n",
                ( pfrom_uid ? pfrom_uid : "" ), ( pfrom_class ? *pfrom_class : -1 ),
                ( pto_uid ? pto_uid : "" ), ( pto_class ? *pto_class : -1 ),
                ( pbp_count ? *pbp_count : -1 ) );
        }
    }

    EIM_free_result( headers, report );
    values.push_back( std::to_string( count ) );
    return POM_ok;
}


/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Queries for a single column of string values.  Retrieved column must be returned "AS strs".
*/
static int select_strs( const char* sql, const char* message, int max_len, int n_binds, EIM_bind_var_t* binds, std::vector<std::string>& values )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    values.clear( );

    EIM_select_var_t select_var[1];
    EIM_select_col( &select_var[0], EIM_varchar, "strs", (max_len > 0 ? max_len : 256), false );
    EIM_exec_sql_bind( sql, &headers, &report, NULL, 1, select_var, n_binds, binds );

    if ( message != NULL )
    {
        EIM_check_error( message );
    }

    if ( report != NULL )
    {
        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            char* char_value = NULL;
            EIM_find_value( headers, row->line, "strs", EIM_varchar, &char_value );

            if ( char_value != NULL )
            {
                values.push_back( char_value );
            }
        }
    }

    EIM_free_result( headers, report );
    return POM_ok;
}

/*------------------------------------------------------------------------------------------------------------------------------*/
/**
    Queries for a single string column and a single integer column.
    Retrieved string column must be returned "AS strs".
    Retrieved int column must be returned "AS ints".
    */
/*
static int select_strs_ints( const char* sql, const char* message, int max_len, int n_binds, EIM_bind_var_t* binds, std::vector<std::string>& ret_strs, std::vector<int>& ret_ints )
{
    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    ret_strs.clear( );
    ret_ints.clear( );

    EIM_select_var_t select_var[2];
    EIM_select_col( &select_var[0], EIM_varchar, "strs", (max_len > 0 ? max_len : 256), false );
    EIM_select_col( &select_var[1], EIM_integer, "ints", sizeof( int ), false );
    EIM_exec_sql_bind( sql, &headers, &report, NULL, 2, select_var, n_binds, binds );

    if ( message != NULL )
    {
        EIM_check_error( message );
    }

    if ( report != NULL )
    {
        for ( EIM_row_p_t row = report; row != 0; row = row->next )
        {
            char* char_value = NULL;
            EIM_find_value( headers, row->line, "strs", EIM_varchar, &char_value );

            if ( char_value != NULL )
            {
                ret_strs.push_back( char_value );
            }

            int* int_value = NULL;
            EIM_find_value( headers, row->line, "ints", EIM_integer, &int_value );

            if ( int_value != NULL )
            {
                ret_ints.push_back( *int_value );
            }
        }
    }

    if ( ret_strs.size( ) != ret_ints.size( ) )
    {
        ERROR_raise( ERROR_line, POM_internal_error, "Query returned a different number of strings (%d) from that of returned ints (%d)", ret_strs.size( ), ret_ints.size( ) );
    }

    EIM_free_result( headers, report );
    return POM_ok;
} */

static int BPV_get_classes( const char* class_name, std::vector<std::string> &cls_name, std::vector<int> &cls_cpid )
{
    cls_name.clear( );
    cls_cpid.clear( );

    int ifail = POM_ok;
    int bv_cnt = 0;
    const char* sql = NULL;

    if ( !class_name )
    {
        sql = "SELECT pname, pcpid FROM PPOM_CLASS ORDER BY pcpid";
    }
    else
    {
        sql = "SELECT pname, pcpid FROM PPOM_CLASS WHERE pname = :1";
        bv_cnt = 1;
    }

    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    EIM_select_var_t vars[2];
    EIM_select_col( &(vars[0]), EIM_varchar, "pname", CLS_NAME_SIZE + 1, false );
    EIM_select_col( &(vars[1]), EIM_integer, "pcpid", sizeof( int ), false );

    EIM_bind_var_t bind_vars[1];

    if ( class_name )
    {
        EIM_bind_val  ( &bind_vars[0], EIM_varchar, strlen( class_name ) + 1, class_name );
    }

    EIM_exec_sql_bind( sql, &headers, &report, "BPV_get_classes() - Unable to read PPOM_class", 2, vars, bv_cnt, bind_vars );

    if ( report != NULL )
    {
        for ( row = report; row != NULL; row = row->next )
        {
            char* name = NULL;
            int* cpid = NULL;

            EIM_find_value( headers, row->line, "pname", EIM_varchar, &name );
            EIM_find_value( headers, row->line, "pcpid", EIM_integer, &cpid );

            if ( !name || !cpid )
            {
                logger( )->printf( "pname=%s", (name ? name : "NULL"));
                logger( )->printf( "pcpid=%d, NULL=%s", (cpid ? *cpid : -99), (cpid ? "false" : "true"));
                ERROR_raise( ERROR_line, POM_internal_error, "Null metadata returned from SQL=%s", sql );
            }

            cls_name.push_back( name );
            cls_cpid.push_back( *cpid );
        }
    }

    EIM_free_result( headers, report );
    return ifail;
}


static int BPV_get_ref_cols_for_object_class(const char* class_name, bool include_no_bp, std::vector<std::string> &tbl, std::vector<std::string> &uid_col, std::vector<std::string> &cls_col)
{
    tbl.clear( );
    uid_col.clear( );
    cls_col.clear( );

    int ifail = POM_op_not_supported;
    const char* sql = NULL;

    int exclude_properties = (include_no_bp ? 1 : 8193);   // if true only exclude transient attributes, otherwise also exclude no-backpointer attributes.

    if ( EIM_dbplat( ) == EIM_dbplat_oracle )
    {
        sql = "WITH "
            "hierarchy (pname, pcpid, ptname, puid, psuperclass) "
            "AS ( "
                "SELECT a.pname, a.pcpid, a.ptname, a.puid, a.psuperclass FROM PPOM_CLASS a where a.pname = :1 "
                "UNION ALL "
                "SELECT b.pname, b.pcpid, b.ptname, b.puid, b.psuperclass FROM PPOM_CLASS b JOIN hierarchy c ON b.pname = c.psuperclass "
            ") "
            "select d.ptname, e.pdbname, e.pptype, e.plength from hierarchy d JOIN PPOM_ATTRIBUTE e ON d.puid = e.rdefining_classu and BITAND(e.pproperties, :2) = 0 and e.pptype in  ( 114, 115 )";
        ifail = POM_ok;
    }
    else if ( EIM_dbplat( ) == EIM_dbplat_mssql )
    {
        sql = "WITH "
            "hierarchy (pname, pcpid, ptname, puid, psuperclass) "
            "AS ( "
                "SELECT a.pname, a.pcpid, a.ptname, a.puid, a.psuperclass FROM PPOM_CLASS a where a.pname = :1 "
                "UNION ALL "
                "SELECT b.pname, b.pcpid, b.ptname, b.puid, b.psuperclass FROM PPOM_CLASS b JOIN hierarchy c ON b.pname = c.psuperclass "
            ") "
            "select d.ptname, e.pdbname, e.pptype, e.plength from hierarchy d JOIN PPOM_ATTRIBUTE e ON d.puid = e.rdefining_classu and (e.pproperties & :2) = 0 and e.pptype in  ( 114, 115 )";
        ifail = POM_ok;
    }
    else if ( EIM_dbplat( ) == EIM_dbplat_postgres )
    {
        sql = "WITH RECURSIVE "
            "hierarchy (pname, pcpid, ptname, puid, psuperclass) "
            "AS ( "
               "SELECT a.pname, a.pcpid, a.ptname, a.puid, a.psuperclass FROM PPOM_CLASS a where a.pname = :1 "
               "UNION ALL "
               "SELECT b.pname, b.pcpid, b.ptname, b.puid, b.psuperclass FROM PPOM_CLASS b JOIN hierarchy c ON b.pname = c.psuperclass "
            ") "
            "select d.ptname, e.pdbname, e.pptype, e.plength from hierarchy d JOIN PPOM_ATTRIBUTE e ON d.puid = e.rdefining_classu and (e.pproperties & :2) = 0 and e.pptype in  ( 114, 115 )";
        ifail = POM_ok;
    }

    EIM_value_p_t headers = NULL;
    EIM_row_p_t report = NULL;
    EIM_row_p_t row;
    EIM_select_var_t vars[4];
    EIM_select_col( &(vars[0]), EIM_varchar, "ptname", CLS_DB_NAME_SIZE + 1, false );
    EIM_select_col( &(vars[1]), EIM_varchar, "pdbname", ATT_DB_NAME_SIZE + 1, false );
    EIM_select_col( &(vars[2]), EIM_integer, "pptype", sizeof( int ), false );
    EIM_select_col( &(vars[3]), EIM_integer, "plength", sizeof( int ), false );

    EIM_bind_var_t bind_vars[2];
    EIM_bind_val  ( &bind_vars[0], EIM_varchar, strlen( class_name ) + 1, class_name );
    EIM_bind_val  ( &bind_vars[1], EIM_integer, sizeof(int), &exclude_properties );

    EIM_exec_sql_bind( sql, &headers, &report, "BPV_get_ref_cols_for_object_class() - Unable to read metadata", 4, vars, 2, bind_vars );

    if ( report != NULL )
    {
        for ( row = report; row != NULL; row = row->next )
        {
            char* tname = NULL;
            char* dbname = NULL;
            int* ptype = NULL;
            int* length = NULL;

            EIM_find_value( headers, row->line, "ptname", EIM_varchar, &tname );
            EIM_find_value( headers, row->line, "pdbname", EIM_varchar, &dbname );
            EIM_find_value( headers, row->line, "pptype", EIM_integer, &ptype );
            EIM_find_value( headers, row->line, "plength", EIM_integer, &length );

            if ( !tname || !dbname || !ptype || !length )
            {
                logger( )->printf( "ptname=%s", (tname ? tname : "NULL"));
                logger( )->printf( "pdbname=%s", (dbname ? dbname : "NULL"));
                logger( )->printf( "pptype=%d, is-NULL=%s", (ptype ? *ptype : -99), (ptype ? "false" : "true"));
                logger( )->printf( "plength=%d, is-NULL=%s", (length ? *length : -99), (length ? "false" : "true"));
                ERROR_raise( ERROR_line, POM_internal_error, "Null metadata returned for class=%s, exclude_properties=%d, SQL=%s", class_name, exclude_properties, sql );
            }

            if ( *length == 1 )
            {
                // Scalar reference
                tbl.push_back( tname );
                uid_col.push_back( fmt__format( "%su", dbname ) );
                cls_col.push_back( fmt__format( "%sc", dbname ) );
            }
            else if( *length == -1 )
            {
                // Process VLA
                tbl.push_back( dbname );
                uid_col.push_back( "pvalu_0" );
                cls_col.push_back( "pvalc_0" );
            }
            else if ( *length > 1 && *length < 7 )
            {
                // Process small array
                for ( int pos = 0; pos < *length; pos++ )
                {
                    tbl.push_back( tname );
                    uid_col.push_back( fmt__format( "%s_%du", dbname, pos ) );
                    cls_col.push_back( fmt__format( "%s_%dc", dbname, pos ) );
                }
            }
            else
            {
                tbl.push_back( dbname );
                uid_col.push_back( "pvalu" );
                cls_col.push_back( "pvalc" );
            }
        }
    }

    EIM_free_result( headers, report );
    return ifail;
}

static int BPV_create_temp_table( char** table_name  )
{
    int ifail = POM_ok;

    // Table already allocated?
    if ( *table_name != NULL )
    {
        return ifail;
    }

    char* tname = NULL;
    ifail = POM_generate_table_name( POM_TEMPORARY_TABLE, "RM_", "BPV_TEMP", &tname );

    if ( ifail )
    {
        ERROR_raise( ERROR_line, ifail, "Unable to generate table name associated with RM_BPV_TEMP" );
    }

#if !defined(PRE_TC131_PLATFORM)
    ifail = EIM_does_table_exist( tname, true );
#else
    ifail = EIM_does_table_exist( tname );
#endif

    if ( !ifail )
    {
        // table already exists... get out
        *table_name = tname;
        return ifail;
    }
    SM_free( (void*)tname );

    int numCols = 5;
    const char* colNames[] = { "from_uid", "from_class", "to_uid", "to_class", "bp_count" };
    int colTypes[]         = { POM_untyped_reference, POM_int, POM_untyped_reference, POM_int, POM_int };
    int colWidths[]        = { EIM_uid_length, sizeof( EIM_uid_t ), EIM_uid_length, sizeof( EIM_uid_t ), sizeof( EIM_uid_t ) };

    ifail = POM_create_table( POM_TEMPORARY_TABLE, "RM_", "BPV_TEMP", numCols, colNames, colTypes, colWidths, POM_TT_CLEAR_ROWS_EOS, table_name );

    if ( ifail )
    {
        ERROR_raise(ERROR_line, ifail, "Unable to create temporary table for RM_BPV_TEMP (ifail = %d)", ifail );
    }

    std::string sql = fmt__format( "CREATE UNIQUE INDEX PIRM_BPV_TEMPIX ON %s (from_uid, to_uid)", *table_name );
    EIM_exec_imm( sql.c_str( ), "Creating PIRM_BPV_TEMPIX index on RM_BPV_TEMP");

    return ifail;
}

static void BPV_clear_temp_table( char* table_name )
{
    std::string sql = fmt__format( "DELETE FROM %s", table_name );
    EIM_exec_imm( sql.c_str(), "BPV_clear_temporary_table(): Clearing temporary table" );
}

static int BPV_insert_refs_into_temp_table( const char* base_tbl, int cpid, const char* ref_tbl, const char* ref_uid_col, const char* ref_cls_col, const char* temp_tbl )
{
    int ifail = POM_ok;
    std::string sql;

    if ( EIM_dbplat( ) == EIM_dbplat_oracle )
    {
        sql = fmt__format( "MERGE INTO %s d "
            "USING (SELECT a.puid AS from_uid, max(%d) AS from_class, a.%s AS to_uid, max(a.%s) AS to_class, count(*) AS bp_count FROM %s a "
            "JOIN %s b ON a.puid = b.puid AND b.ppid = %d WHERE a.%s IS NOT NULL AND a.%s <> 'AAAAAAAAAAAAAA' GROUP BY a.puid, a.%s) c "
            "ON (c.from_uid = d.from_uid AND c.to_uid = d.to_uid) "
            "WHEN MATCHED THEN "
            "UPDATE SET d.bp_count = (d.bp_count + c.bp_count) "
            "WHEN NOT MATCHED THEN "
            "INSERT (from_uid, from_class, to_uid, to_class, bp_count) "
            "VALUES (c.from_uid, c.from_class, c.to_uid, c.to_class, c.bp_count)",
            temp_tbl, cpid, ref_uid_col, ref_cls_col, ref_tbl, base_tbl, cpid, ref_uid_col, ref_uid_col, ref_uid_col );
    }
    else if ( EIM_dbplat( ) == EIM_dbplat_mssql )
    {
        sql = fmt__format( "MERGE INTO %s d "
            "USING (SELECT a.puid AS from_uid, max(%d) AS from_class, a.%s AS to_uid, max(a.%s) AS to_class, count(*) AS bp_count FROM %s a "
            "JOIN %s b ON a.puid = b.puid AND b.ppid = %d WHERE a.%s IS NOT NULL AND a.%s <> 'AAAAAAAAAAAAAA' GROUP BY a.puid, a.%s) c "
            "ON (c.from_uid = d.from_uid AND c.to_uid = d.to_uid) "
            "WHEN MATCHED THEN "
            "UPDATE SET d.bp_count = (d.bp_count + c.bp_count) "
            "WHEN NOT MATCHED THEN "
            "INSERT (from_uid, from_class, to_uid, to_class, bp_count) "
            "VALUES (c.from_uid, c.from_class, c.to_uid, c.to_class, c.bp_count);",
            temp_tbl, cpid, ref_uid_col, ref_cls_col, ref_tbl, base_tbl, cpid, ref_uid_col, ref_uid_col, ref_uid_col );
    }
    else if ( EIM_dbplat( ) == EIM_dbplat_postgres )
    {
        sql = fmt__format( "INSERT INTO %s AS d (from_uid, from_class, to_uid, to_class, bp_count) "
            "(SELECT a.puid AS from_uid, max(%d) AS from_class, a.%s AS to_uid, max(a.%s) AS to_class, count(*) AS bp_count "
            "FROM %s a JOIN %s b ON a.puid = b.puid AND b.ppid = %d WHERE a.%s IS NOT NULL AND a.%s <> 'AAAAAAAAAAAAAA' GROUP BY a.puid, a.%s) "
            "ON CONFLICT (from_uid, to_uid) DO UPDATE SET bp_count = (d.bp_count + EXCLUDED.bp_count)",
            temp_tbl, cpid, ref_uid_col, ref_cls_col, ref_tbl, base_tbl, cpid, ref_uid_col, ref_uid_col, ref_uid_col );
    }

    EIM_exec_imm( sql.c_str( ), "Merging, references to be validated, into temporary table" );

    return ifail;
}

static int validate_bp2_op( int* found_count )
{
    int ifail = POM_ok;
    std::vector<std::string>  cls_names;
    std::vector<int>          cls_cpids;
    char* tmp_tbl = NULL;
    int classes_with_problems = 0;
    int processed_classes = 0;
    int processed_columns = 0;
    int processed_last_cpid = -1;

    if ( found_count )
    {
        *found_count = 0;
    }

    // Get classes to process
    ifail = BPV_get_classes( args->class_n, cls_names, cls_cpids );

    if ( ifail )
    {
        ERROR_raise( ERROR_line, ifail, "Unable to get target classes to process" );
    }

    if ( cls_names.size( ) < 1 )
    {
        ifail = POM_invalid_value;

        if ( args->class_n )
        {
            cons_out( fmt__format( "\nClass %s does not exist", (args->class_n) ) );
        }
        else
        {
            cons_out( fmt__format( "\nUnable to find classes to process" ) );
        }
        return ifail;
    }

    ifail = BPV_create_temp_table( &tmp_tbl );

    if ( ifail || tmp_tbl == NULL )
    {
        ERROR_raise( ERROR_line, ifail, "Unable to create temporary table RM_BPV_TEMP" );
    }

    // BPV:,class,cpid,Invalid_from_class,Invalid_bp_count,Missing_bp,Unneeded_bp,Comment,
    logger()->printf( "BPV:Summary,Class,Cpid,Invalid_from_class,Invalid_bp_count,Missing_bp,Unneeded_bp,Comment,\n" );
    
    // Start processing target classes. 
    for ( int i = 0; i < cls_names.size( ); i++ )
    {
        processed_classes++;

        // Identify the base query class-table, it contains the ppid column for the target class.
        // std::string base_query_class = DMS_base_query_class_by_name( cls_names[i].c_str( ) );
#if !defined(PRE_TC12_PLATFORM)
        int base_query_cpid = DMS_get_top_query_class( cls_cpids[i] );
        OM_class_t base_query_id = DDS_class_id_of_pid( base_query_cpid );
        std::string base_query_tbl = DDS_table_name( base_query_id );
#else
        std::string base_query_tbl = get_top_query_table( cls_cpids[i] );
#endif  

        // Identify all the reference columns, minus the no-backpointer columns, for this object class. 
        std::vector<std::string> ref_table;
        std::vector<std::string> ref_uid_col;
        std::vector<std::string> ref_cls_col;

        ifail = BPV_get_ref_cols_for_object_class( cls_names[i].c_str( ), false, ref_table, ref_uid_col, ref_cls_col );

        if ( ifail )
        {
            ERROR_raise( ERROR_line, ifail, "Unable to get reference columns for object class %s", cls_names[i].c_str( ) );
        }

        processed_last_cpid = cls_cpids[i];

        if ( ref_table.size() > 0 )
        {
            // Add all references into temporary table.
            for ( int j = 0; j < ref_table.size(); j++ )
            {
                processed_columns++;

                ifail = BPV_insert_refs_into_temp_table( base_query_tbl.c_str(), cls_cpids[i], ref_table[j].c_str(), ref_uid_col[j].c_str(), ref_cls_col[j].c_str(), tmp_tbl );

                if ( ifail )
                {
                    ERROR_raise( ERROR_line, ifail, "Unable insert references into temporary table from source table %s", ref_table[j].c_str() );
                }
            }

            // Gather issues associated with the object class.
            std::string sql;
            std::string invalid_from_classes = "0";
            std::string invalid_bp_counts = "0";
            std::string missing_bps = "0";
            std::string unneeded_bps = "0";
            std::vector<std::string> values;

            int tmp_cpid = cls_cpids[i];
            EIM_bind_var_t bind_vars[2];

            switch ( EIM_dbplat() )
            {
            case EIM_dbplat_oracle:
                EIM_bind_val( &bind_vars[0], EIM_integer, sizeof( int ), &tmp_cpid );
                EIM_bind_val( &bind_vars[1], EIM_integer, sizeof( int ), &tmp_cpid );

                // Invalid from_class values
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT TO_CHAR(COUNT(*)) AS strs FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2", base_query_tbl.c_str() );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", MAX_COUNT_CHAR_SIZE + 2, 2, bind_vars, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2", base_query_tbl.c_str() );
                    log_bps_w_invalid_from_class( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", 2, bind_vars, tmp_cpid, values );               
                }
                invalid_from_classes = values[0];

                // Invalid bp_counts
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT TO_CHAR(COUNT(*)) AS strs FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count, b.bp_count AS correct_count FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", tmp_tbl );
                    log_bps_w_invalid_counts( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", 0, NULL, values );
                }
                invalid_bp_counts = values[0];

                // Missing backpointers
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT TO_CHAR(COUNT(*)) AS strs FROM ( "
                                       "SELECT from_uid, to_uid FROM %s MINUS SELECT from_uid, to_uid FROM POM_BACKPOINTER)", tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding missing backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM %s a "
                                       "LEFT JOIN POM_BACKPOINTER b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE b.from_uid IS NULL", tmp_tbl );
                    log_missing_bps( sql.c_str(), "validate_bp2_op(): Finding missing backpointers", 0, NULL, values );
                }
                missing_bps = values[0];

                // Find unneeded backpointers
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT TO_CHAR(COUNT(*)) AS strs FROM ( "
                                       "SELECT from_uid, to_uid FROM POM_BACKPOINTER WHERE from_class = %d MINUS SELECT from_uid, to_uid FROM %s )", tmp_cpid, tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER a "
                                       "LEFT JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.from_class = %d and b.from_uid IS NULL",
                        tmp_tbl, tmp_cpid );
                    log_unneeded_bps( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", 0, NULL, values );
                }
                unneeded_bps = values[0];
                break;

            case EIM_dbplat_mssql:
                EIM_bind_val( &bind_vars[0], EIM_integer, sizeof( int ), &tmp_cpid );
                EIM_bind_val( &bind_vars[1], EIM_integer, sizeof( int ), &tmp_cpid );

                // Invalid from_class values
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT CAST(COUNT_BIG(*) as varchar(%d)) AS strs FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2",
                        MAX_COUNT_CHAR_SIZE, base_query_tbl.c_str() );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", MAX_COUNT_CHAR_SIZE + 2, 2, bind_vars, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2",
                        base_query_tbl.c_str() );
                    log_bps_w_invalid_from_class( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", 2, bind_vars, tmp_cpid, values );               
                }
                invalid_from_classes = values[0];

                // Invalid bp_counts
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT CAST(COUNT_BIG(*) as varchar(%d)) AS strs FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", MAX_COUNT_CHAR_SIZE, tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count, b.bp_count AS correct_count FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", tmp_tbl );
                    log_bps_w_invalid_counts( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", 0, NULL, values );
                }
                invalid_bp_counts = values[0];

                // Missing backpointers
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT CAST(COUNT_BIG(*) as varchar(%d)) AS strs FROM ( "
                                       "SELECT from_uid, to_uid FROM %s EXCEPT SELECT from_uid, to_uid FROM POM_BACKPOINTER ) AS c",
                        MAX_COUNT_CHAR_SIZE, tmp_tbl );
                    select_strs( sql.c_str(), "Finding missing backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM %s a "
                                       "LEFT JOIN POM_BACKPOINTER b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE b.from_uid IS NULL",
                        tmp_tbl );
                    log_missing_bps( sql.c_str(), "validate_bp2_op(): Finding missing backpointers", 0, NULL, values );
                }
                missing_bps = values[0];

                // Find unneeded backpointers
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT CAST(COUNT_BIG(*) as varchar(%d)) AS strs FROM ( "
                                       "SELECT from_uid, to_uid FROM POM_BACKPOINTER WHERE from_class = %d EXCEPT SELECT from_uid, to_uid FROM %s ) AS c",
                        MAX_COUNT_CHAR_SIZE, tmp_cpid, tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER a "
                                       "LEFT JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.from_class = %d and b.from_uid IS NULL",
                        tmp_tbl, tmp_cpid );
                    log_unneeded_bps( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", 0, NULL, values );
                }
                unneeded_bps = values[0];
                break;

            case EIM_dbplat_postgres:
                EIM_bind_val( &bind_vars[0], EIM_integer, sizeof( int ), &tmp_cpid );
                EIM_bind_val( &bind_vars[1], EIM_integer, sizeof( int ), &tmp_cpid );

                // Invalid from_class values
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT COUNT(*)::text AS strs FROM POM_BACKPOINTER AS a "
                                       "JOIN %s AS b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2",
                        base_query_tbl.c_str() );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", MAX_COUNT_CHAR_SIZE + 2, 2, bind_vars, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER AS a "
                                       "JOIN %s AS b ON a.from_uid = b.puid and b.ppid = :1 WHERE a.from_class <> :2",
                        base_query_tbl.c_str() );
                    log_bps_w_invalid_from_class( sql.c_str(), "validate_bp2_op(): Finding invalid_from_classes", 2, bind_vars, tmp_cpid, values );
                }
                invalid_from_classes = values[0];

                // Invalid bp_counts
                if ( !args->log_details )
                {
                    sql = fmt__format( "SELECT COUNT(*)::text AS strs FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count, b.bp_count AS correct_count FROM POM_BACKPOINTER a "
                                       "JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.bp_count <> b.bp_count", tmp_tbl );
                    log_bps_w_invalid_counts( sql.c_str(), "validate_bp2_op(): Finding invalid_bp_counts", 0, NULL, values );
                }
                invalid_bp_counts = values[0];

                // Missing backpointers
                if ( !args->log_details )
                {
                    sql = fmt__format( "WITH cte( from_uid, to_uid ) AS ( "
                                       "SELECT from_uid, to_uid FROM %s EXCEPT SELECT from_uid, to_uid FROM POM_BACKPOINTER ) "
                                       "SELECT COUNT(*)::text AS strs FROM cte", tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding missing backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM %s a "
                                       "LEFT JOIN POM_BACKPOINTER b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE b.from_uid IS NULL",
                        tmp_tbl );
                    log_missing_bps( sql.c_str(), "validate_bp2_op(): Finding missing backpointers", 0, NULL, values );
                }
                missing_bps = values[0];

                // Find unneeded backpointers   
                if ( !args->log_details )
                {
                    sql = fmt__format( "WITH cte( from_uid, to_uid ) AS ( "
                                       "SELECT from_uid, to_uid FROM POM_BACKPOINTER WHERE from_class = %d EXCEPT SELECT from_uid, to_uid FROM %s ) "
                                       "SELECT COUNT(*)::text AS strs FROM cte", tmp_cpid, tmp_tbl );
                    select_strs( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", MAX_COUNT_CHAR_SIZE + 2, 0, NULL, values );
                }
                else
                {
                    sql = fmt__format( "SELECT a.from_uid, a.from_class, a.to_uid, a.to_class, a.bp_count FROM POM_BACKPOINTER a "
                                       "LEFT JOIN %s b ON a.from_uid = b.from_uid and a.to_uid = b.to_uid WHERE a.from_class = %d and b.from_uid IS NULL",
                        tmp_tbl, tmp_cpid );
                    log_unneeded_bps( sql.c_str(), "validate_bp2_op(): Finding uneeded backpointers", 0, NULL, values );
                }
                unneeded_bps = values[0];
                break;

            default:
                ERROR_internal( ERROR_line, "Unrecognized EIM_dbplat value" );
            }

            // Check for problems within the class just processed.

            if ( strcmp( invalid_from_classes.c_str(), "0" ) != 0 || strcmp( invalid_bp_counts.c_str(), "0" ) != 0 || strcmp( missing_bps.c_str(), "0" ) != 0 || strcmp( unneeded_bps.c_str(), "0" ) != 0 )
            {
                classes_with_problems++;

                static bool header_to_console = false;
                if( !header_to_console )
                {
                    cons_out_no_log( "BPV:Summary,Class,Cpid,Invalid_from_class,Invalid_bp_count,Missing_bp,Unneeded_bp,Comment," );
                    header_to_console = true;               
                }

                // BPV:,class,cpid,Invalid_from_class,Invalid_bp_count,Missing_bp,Unneeded_bp,Comment,
                std::string msg = fmt__format( "BPV:Summary,%s,%d,%s,%s,%s,%s,,", cls_names[i].c_str(), cls_cpids[i],
                    invalid_from_classes.c_str(), invalid_bp_counts.c_str(), missing_bps.c_str(), unneeded_bps.c_str() );

                cons_out_no_log( msg );
                logger()->printf( "%s\n", msg.c_str() );

                // Let's update the output count. 
                // This is only used for AOS testing so we don't really have to worry about anything bigger than an it. 
                if ( found_count )
                {
                    long long int temp = std::stoll( invalid_from_classes, nullptr, 10 );
                    *found_count += (int)(temp & 0x7fffffff);

                    temp = std::stoll( invalid_bp_counts, nullptr, 10 );
                    *found_count += (int)(temp & 0x7fffffff);

                    temp = std::stoll( missing_bps, nullptr, 10 );
                    *found_count += (int)(temp & 0x7fffffff);

                    temp = std::stoll( unneeded_bps, nullptr, 10 );
                    *found_count += (int)(temp & 0x7fffffff);
                }
            }
        }
        // Clean up after the class we just processed.
        BPV_clear_temp_table( tmp_tbl );
        EIM_commit_transaction( "One-transaction per object class." );
        EIM_start_transaction();

        if( ( processed_classes % 200 ) == 0 )
        {
            cons_out( fmt__format( "Processed %d of %d classes, last CPID = %d", processed_classes, cls_names.size(), processed_last_cpid ) );       
        }
    }

    cons_out( fmt__format( "\nClasses with an issue: %d", classes_with_problems ) );
    cons_out( fmt__format( "Processed classes:     %d", processed_classes ) );
    cons_out( fmt__format( "Processed columns:     %d", processed_columns ) );
    cons_out( fmt__format( "Last CPID:             %d", processed_last_cpid ) );
    if ( classes_with_problems > 0 && args->log_details)
    {
        cons_out( fmt__format( "Search syslog for \"BPV:\" for additional information" ) );
    }

    return ifail;
}

/* ********************************************************************************
** END OF: validate_bp2_op() routines.
** *******************************************************************************/

/* ********************************************************************************
** START OF: validate_cids_op() routines.
** *******************************************************************************/

/*-----------------------------------------------------------------*/
static void output_att_ref_cid_data( const char prefix, const cls_t* cls, const cls_t* flat, const att_t* att )
{
    std::stringstream out_msg;

    if ( args->min_flag )
    {
        out_msg << "\n ";
    }
    else
    {
        if ( prefix == VSR_DATA_LINE )
        {
            out_msg << "\n"
                    << VSR_HDR_LINE;
        }
        else
        {
            out_msg << "\n"
                    << prefix;
        }
    }

    std::string storage_mode = get_storage_mode( flat != NULL ? flat->name : cls->name );

    if ( flat == NULL )
    {
        out_msg << "  " << storage_mode << " " << cls->name << ":" << att->name << "[" << att->pptype << "] (" << get_ref_table_and_column( cls, flat, att, -1, true ) << "): " << att->uid_cnt << " class IDs found";
    }
    else
    {
        out_msg << "  " << storage_mode << " " << flat->name << ":" << flat->name << "\\" << cls->name << ":" << att->name << "[" << att->pptype << "] (";
        out_msg << get_ref_table_and_column( cls, flat, att, -1, true ) << "): " << att->uid_cnt << " class IDs found";
    }

    if ( args->min_flag )
    {
        lprintf( "%s\n", out_msg.str().c_str() );
    }
    else
    {
        cons_out( out_msg.str() );
    }


    ref_cpid_t* uids = (ref_cpid_t*)att->uids;
/*
    for ( int i = 0; i < att->uid_cnt; i++ )
    {
        out_msg << "\n        " << uids[i].uid << " " << uids[i].ref_cid << " " << uids[i].tar_cid;
    }
*/
    int col_offset = 0;
    int max_offset = 1;

    if (isSA(att))
    {
        max_offset = att->plength;
    }

    for (; col_offset < max_offset; col_offset++)
    {
        std::string ref_tbl = get_ref_table(cls, flat, att);             // reference table
        std::string ref_uid_col = get_ref_column(att, col_offset);       // reference UID column
        std::string ref_cid_col = get_ref_cid_column(att, col_offset);   // reference class ID column

        for ( int i = 0; i < att->uid_cnt; i++ )
        {
            char* data_msg = SM_sprintf( "    VCIDS:UPDATE %s SET %s = %d WHERE %s = '%s' AND %s = %d;",
                ref_tbl.c_str(), ref_cid_col.c_str(), uids[i].tar_cid, ref_uid_col.c_str(), uids[i].uid, ref_cid_col.c_str(), uids[i].ref_cid );

            // out_msg << "\n    VCIDS:UPDATE " << ref_tbl << " SET " << ref_cid_col << " = " << uids[i].tar_cid << " WHERE ";
            // out_msg << ref_uid_col << " = '" << uids[i].uid << "' AND " << ref_cid_col << " = " << uids[i].ref_cid;

            if ( args->min_flag )
            {
                lprintf( "%s\n", data_msg );
            }
            else
            {
                cons_out( data_msg );
            }

            SM_free( data_msg );
        }
    }
}


/*----------------------------------------------------------------------- -
** Get UIDs with invalid class IDs
** ---------------------------------------------------------------------- - */
static int get_ref_cids( const char* target_ppid_tbl, cls_t* cls, const cls_t* flat, att_t* att, int max_records )
{
    int ifail = OK;
    int record_cnt = max_records;

    ERROR_PROTECT
    att->uids = NULL;
    att->uid_cnt = 0;

    int col_offset = 0;
    int max_offset = 1; 

    if ( isSA( att ) )
    {
        max_offset = att->plength;
    }

    for ( ; col_offset < max_offset && (record_cnt > 0 || max_records == -1); col_offset++ )
    {
        EIM_select_var_t vars[3];
        EIM_value_p_t headers = NULL;
        EIM_row_p_t report = NULL;
        EIM_row_p_t row;
        int row_cnt = 0;
        ref_cpid_t* alloc_uids = NULL;

        std::string ref_tbl = get_ref_table( cls, flat, att );             // reference table
        std::string ref_uid_col = get_ref_column( att, col_offset );       // reference UID column
        std::string ref_cid_col = get_ref_cid_column( att, col_offset );   // reference class ID column

        std::stringstream sql;
        sql << "SELECT ";

        if ( !args->noparallel_flag && EIM_dbplat( ) == EIM_dbplat_oracle )
        {
            sql << "/*+ parallel */ ";
        }

        sql << "distinct ";

        if ( EIM_dbplat( ) == EIM_dbplat_mssql )
        {
            sql << "TOP " << (args->max_ref_cnt) << " ";
        }

        sql << "a." << ref_uid_col << " as puid, a." << ref_cid_col << " as ref, b.ppid as tar FROM " << ref_tbl << " a ";

        sql << "INNER JOIN " << target_ppid_tbl << " b ON a." << ref_uid_col << " = b.puid ";

        sql << "WHERE a." << ref_cid_col << " <> b.ppid";

        if ( EIM_dbplat( ) == EIM_dbplat_oracle )
        {
            sql << " AND rownum <= " << (args->max_ref_cnt);
        }
        if ( EIM_dbplat( ) == EIM_dbplat_postgres )
        {
            sql << " FETCH FIRST " << (args->max_ref_cnt) << " ROWS ONLY";
        }

        EIM_select_col( &(vars[0]), EIM_varchar, "puid", MAX_UID_SIZE, false );
        EIM_select_col( &(vars[1]), EIM_integer, "ref", sizeof( int ), false );
        EIM_select_col( &(vars[2]), EIM_integer, "tar", sizeof( int ), false );
        ifail = EIM_exec_sql_bind( sql.str( ).c_str( ), &headers, &report, 0, 3, vars, 0, NULL );

        if ( !args->ignore_errors_flag )
        {
            EIM_check_error( "get_ref_cids()\n" );
        }
        else if ( ifail != OK )
        {
            EIM_clear_error( );

            std::stringstream msg;
            msg << "\nError " << ifail << " while reading " << get_ref_table_and_column( cls, NULL, att, -1, true );
            msg << ". SKIPPING class:attribute " << cls->name << ":" << att->name << ".\n";
            cons_out( msg.str( ) );

            EIM_free_result( headers, report );
            report = NULL;
            headers = NULL;
        }

        if ( report != NULL )
        {
            row_cnt = 0;

            for ( row = report; row != NULL; row = row->next ) row_cnt++;

            record_cnt -= row_cnt;
            alloc_uids = (ref_cpid_t*)SM_realloc( att->uids, ( sizeof( ref_cpid_t ) * ( att->uid_cnt + row_cnt ) ) );
            att->uids = NULL;

            int off = att->uid_cnt;

            for ( row = report; row != NULL; row = row->next )
            {
                alloc_uids[off].uid[0] = '\0';
                alloc_uids[off].ref_cid = 0;
                alloc_uids[off].tar_cid = 0;

                char* tmp = NULL;
                EIM_find_value( headers, row->line, "puid", EIM_varchar, &tmp );
                strncpy( alloc_uids[off].uid, tmp, MAX_UID_SIZE );
                alloc_uids[off].uid[MAX_UID_SIZE] = '\0';

                int* cpid = NULL;
                EIM_find_value( headers, row->line, "ref", EIM_integer, &cpid );
                alloc_uids[off].ref_cid = *cpid;

                cpid = NULL;
                EIM_find_value( headers, row->line, "tar", EIM_integer, &cpid );
                alloc_uids[off].tar_cid = *cpid;

                off++;
            }

            att->uids = (ref_t*)alloc_uids;
            att->uid_cnt += row_cnt;

            if ( flat == NULL )
            {
                cls->ref_cnt += row_cnt;
            }
            else
            {
                cls->flt_cnt += row_cnt;
            }
            EIM_free_result( headers, report );
        }
    }

    ERROR_RECOVER
    const std::string msg( "EXCEPTION [get_ref_cids()]: See syslog for additional details. (See -i option to ignore this error.)" );
    cons_out( msg );

    ERROR_reraise( );

    ERROR_END

    return(ifail);
}

/*------------------------------------------------------------------------
** Searches (non-flattened) reference attributes for class-ID values that
** do not match class ID (ppid) values in the target class table.
** Once problem class IDs are found the information is dumped to the console.
** ----------------------------------------------------------------------- */
static int output_ref_cids( const char* target_ppid_tbl, cls_t* cls, int* accum_cnt, int* attr_cnt, char** last_attr_name )
{
    int ifail = OK;

    if ( *accum_cnt < args->max_ref_cnt )
    {

        int att_cnt = cls->att_cnt;

        for ( int j = 0; j < att_cnt && *accum_cnt < args->max_ref_cnt; j++ )
        {
            int remaining = args->max_ref_cnt - *accum_cnt;
            int lcl_ifail = get_ref_cids( target_ppid_tbl, cls, NULL, &cls->atts[j], (remaining > 0 ? remaining : 0) );

            if ( lcl_ifail != OK )
            {
                output_att_err( cls, NULL, &cls->atts[j], lcl_ifail, "Skipping attribute" );

                if ( ifail == OK )
                {
                    ifail = lcl_ifail;
                }
                continue;
            }

            (*last_attr_name) = cls->atts[j].name;

            if ( cls->atts[j].uid_cnt > 0 )
            {
                output_att_ref_cid_data( VSR_DATA_LINE, cls, NULL, &cls->atts[j] );
                *accum_cnt += cls->atts[j].uid_cnt;
                // Free up memory used to temporary hold UIDs. 
                SM_free( cls->atts[j].uids );
                cls->atts[j].uids = NULL;
                cls->atts[j].uid_cnt = 0;
            }
            else if ( args->debug_flag == TRUE || args->verbose_flag == TRUE )
            {
                output_att_msg( cls, NULL, &cls->atts[j], "0 class IDs found" );
            }

            (*attr_cnt)++;

            int remainder = (*attr_cnt) % 100;

            if ( remainder == 0 )
            {
                std::stringstream msg;
                msg << "Attributes processed = " << *attr_cnt << " of " << args->att_cnt << ". Problem class IDs found = " << *accum_cnt;
                cons_out( msg.str( ) );
            }
        }
    }

    return ifail;
}


/*------------------------------------------------------------------------
** Searches flattened attributes for class-ID values that
** do not match class ID (ppid) values in the target class table.
** Once problem class IDs are found the information is dumped to the console.
** ----------------------------------------------------------------------- */
static int output_flattened_ref_cids( const char* target_ppid_tbl, const std::vector< hier_t >& hier, std::vector< cls_t >& meta, cls_t* flat, int* accum_cnt, int* attr_cnt )
{
    int ifail = OK;

    if ( isFlat( flat ) )
    {
        if ( *accum_cnt <= args->max_ref_cnt )
        {
            // Process the attributes that have been flattened into this class
            cls_t* cur_cls = NULL;
            int cur_cpid = flat->cls_id;
            int par_cpid = hier[cur_cpid].par_id;

            // Walk up the hiearchy until we come to POM_object (cpid = 1)

            while ( *accum_cnt <= args->max_ref_cnt && par_cpid > 0 && hier[par_cpid].cls_pos >= 0 )
            {
                int pos = hier[par_cpid].cls_pos;
                cur_cls = &meta[pos];
                cur_cpid = par_cpid;
                par_cpid = hier[cur_cpid].par_id;

                int att_cnt = cur_cls->att_cnt;

                for ( int j = 0; j < att_cnt && *accum_cnt <= args->max_ref_cnt; j++ )
                {
                    // Process only attributes found on the class table.
                    if ( !isScalar( &cur_cls->atts[j] ) && !isSA( &cur_cls->atts[j] ) )
                    {
                        continue;
                    }
                    int remaining = args->max_ref_cnt - *accum_cnt;
                    int lcl_ifail = get_ref_cids( target_ppid_tbl, cur_cls, flat, &cur_cls->atts[j], ( remaining > 0 ? remaining : 0 ) );

                    if ( lcl_ifail != OK )
                    {
                        output_att_err( cur_cls, flat, &cur_cls->atts[j], lcl_ifail, "Skipping attribute" );

                        if ( ifail == OK )
                        {
                            ifail = lcl_ifail;
                        }
                        continue;
                    }

                    (*attr_cnt)++;

                    if ( cur_cls->atts[j].uid_cnt > 0 )
                    {
                        output_att_ref_cid_data( VSR_DATA_LINE, cur_cls, flat, &cur_cls->atts[j] );
                        *accum_cnt += cur_cls->atts[j].uid_cnt;
                        // Free up memory used to temporary hold UIDs. 
                        SM_free( cur_cls->atts[j].uids );
                        cur_cls->atts[j].uids = NULL;
                        cur_cls->atts[j].uid_cnt = 0;
                    }
                    else if ( args->debug_flag == TRUE || args->verbose_flag == TRUE )
                    {
                        output_att_msg( cur_cls, flat, &cur_cls->atts[j], "0 class IDs found" );
                    }
                }
            }
        }
    }

    return ifail;
}

static int validate_cids_op( int* found_count )
{
    int ifail = POM_ok;
    std::vector< std::string > flattened_classes;

    // Check that the user has specified a validation class. (-vc=<class_name>)
    if (!args->val_class_n)
    {
        ifail = POM_invalid_string;
        std::stringstream msg;
        msg << "\nError " << ifail << " The validation class name (-vc=<class_name>) must be specified.";
        cons_out(msg.str());
    }
    else
    {
        // Check that the user has specified a validation class that contains the ppid column. (-vc=<class_name>)
        if (strcmp("POM_object", args->val_class_n) != 0)
        {
            get_flattened_class_names(flattened_classes);
            ifail = POM_invalid_string;

            for (int i = 0; i < flattened_classes.size(); i++)
            {
                if (strcmp(args->val_class_n, flattened_classes[i].c_str()) == 0)
                {
                    ifail = POM_ok;
                    break;
                }
            }

            if (ifail)
            {
                std::stringstream msg;
                msg << "\nError " << ifail << " An invalid validation class name (-vc=<class_name>) has been specified. (" << args->val_class_n << ").";
                cons_out(msg.str());
            }
        }
    }

    if (ifail)
    {
        // Let user know what are the acceptable classes.
        if (flattened_classes.size() < 1)
        {
            get_flattened_class_names(flattened_classes);
        }
        std::stringstream msg;
        msg << "\n       One of the following classes MUST be specified with the -vc=<class_name> parameter";
        msg << "\n       POM_object";

        for (int i = 0; i < flattened_classes.size(); i++)
        {
            msg << "\n       " << flattened_classes[i];
        }

        msg << "\n\n       Note: For complete coverage the utility should be run once with each of these classes.";
        cons_out(msg.str());

        return ifail;
    }

    const char* target_ppid_tbl = NULL;
    ifail = get_class_table_name( args->val_class_n, &target_ppid_tbl );

    if (ifail || !target_ppid_tbl)
    {
        std::stringstream msg;
        msg << "\nError " << ifail << " Unable to identify class table for class " << args->val_class_n;
        cons_out(msg.str());

        if( !ifail )
        {
            ifail = POM_invalid_string;
        }
        return ifail;
    }

    {
        std::stringstream msg;
        msg << "\nValidation class = " << args->val_class_n << " / table = " << target_ppid_tbl;
        cons_out(msg.str());
    }

    /* Get class hierarchy */
    std::vector< hier_t > hier;
    getHierarchy( hier );

    /* Get system metadata of all typed and untyped references */
    std::vector< cls_t > meta;
    getRefMeta( meta, hier );
    // Append fake metadata so normal tables, like POM_BACKPOINTER, get processed as well. 
    appendTableRefMeta( meta, hier );

    if ( args->debug_flag )
    {
        dumpHierarchy( hier );
        dumpRefMetadata( meta );
    }

    /* Loop through each class and all reference attributes */
    /* looking for references with invalid class IDs.       */
    int ref_cnt = 0;
    int class_cnt = meta.size( );
    int att_processed = 0;
    int flat_att_processed = 0;
    char* last_class = NULL;
    char* last_att = NULL;
    int  lcl_ifail = OK;

    if ( !args->class_obj_flag )
    {
        /* Process all loaded attributes. */
        for ( int i = 0; i < class_cnt && ref_cnt <= args->max_ref_cnt; i++ )
        {
            last_class = meta[i].name;

            // Search normal class attributes and output the information.
            lcl_ifail = output_ref_cids( target_ppid_tbl, &meta[i], &ref_cnt, &att_processed, &last_att );

            if ( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }

            // Search flattened attributes and output the information.
            lcl_ifail = output_flattened_ref_cids( target_ppid_tbl, hier, meta, &meta[i], &ref_cnt, &flat_att_processed );

            if ( ifail == OK && lcl_ifail != OK )
            {
                ifail = lcl_ifail;
            }
        }
    }
    else
    {
        /* The class_obj_flag indicates that we are looking for a reference in a specific object hierarchy. */
        int starting_cpid = get_cpid( args->class_obj_n );

        if ( starting_cpid <= 0 )
        {
            ifail = POM_invalid_string;
            std::stringstream msg;
            msg << "The class name on the -o option (" << args->class_obj_n << ") is invalid";
            ifail = error_out( ERROR_line, ifail, msg.str( ) );
        }
        else
        {
            /* process the specified class and its parent classes. */
            cls_t* class_p = NULL;

            /* Move up the hiearchy to the first class that has references */
            int cls_id = starting_cpid;
            hier_t* hptr = NULL;

            while ( cls_id > 0 )
            {
                hptr = &hier[cls_id];

                /* Does class have attributes with references?*/
                if ( hptr->refs > 0 && hptr->cls_pos >= 0 )
                {
                    class_p = &meta[hptr->cls_pos];
                    break;
                }
                cls_id = hptr->par_id;
            }

            /* Did we find a class that contains a reference attribute?*/
            if ( class_p == NULL )
            {
                cons_out( "\nNo classes were found that contain reference attributes." );
            };

            /* Query the reference attribute and then move up the hiearchy. */
            while ( class_p != NULL )
            {
                last_class = class_p->name;

                // Search normal class attributes and output the information.
                lcl_ifail = output_ref_cids( target_ppid_tbl, class_p, &ref_cnt, &att_processed, &last_att );

                if ( ifail == OK && lcl_ifail != OK )
                {
                    ifail = lcl_ifail;
                }

                // Search flattened attributes and output the information.
                lcl_ifail = output_flattened_ref_cids( target_ppid_tbl, hier, meta, class_p, &ref_cnt, &flat_att_processed );

                if ( ifail == OK && lcl_ifail != OK )
                {
                    ifail = lcl_ifail;
                }

                //
                // Move to next class up the hierarchy.
                //
                cls_id = class_p->cls_id;
                class_p = NULL;
                hier_t* hptr = NULL;

                while ( cls_id > 0 )
                {
                    hptr = &hier[cls_id];
                    cls_id = hptr->par_id;
                    hptr = &hier[cls_id];

                    if ( hptr->refs > 0 && hptr->cls_pos >= 0 )
                    {
                        class_p = &meta[hptr->cls_pos];
                        break;
                    }
                }
            }
        }
    }

    if ( last_class != NULL && last_att != NULL )
    {
        std::stringstream msg;
        msg << "\nLast attribute processed is " << last_class << ":" << last_att;
        cons_out( msg.str( ) );
    }

    if ( found_count)
    {
        // We always retrieve 1 more than the maximum
        *found_count = ref_cnt;
    }

    std::stringstream msg;
    msg << "\nTotal system reference attributes         = " << args->att_cnt;
    msg << "\nNormal reference attributes processed     = " << att_processed;
    msg << "\nFlattened reference attributes processed  = " << flat_att_processed;
    msg << "\nReferences with bad class IDs found       = " << ref_cnt;
    cons_out( msg.str( ) );

    return(ifail);
}

/* ********************************************************************************
** END OF: validate_cids_op() routines.
** *******************************************************************************/
