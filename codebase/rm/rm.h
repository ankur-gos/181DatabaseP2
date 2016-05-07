
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator(FileHandle fh,
                  vector<Attribute> *recordDescriptor,
                  const string &conditionAttribute,
                  const CompOp compOp,                  
                  const void *value,                    
                  const vector<string> &attributeNames,
                  RecordBasedFileManager &rbfm) 
                  {
                    fh = fh;
                    rbfm->scan(fh, recordDescriptor, conditionAttribute, 
                               compOp, value, attributeNames, rbfm_ScanIterator);
  };

  ~RM_ScanIterator() {};

  FileHandle fh;
  vector<Attribute> *recordDescriptor;
  vector<string> *conditionAttribute;
  CompOp compOp;
  void *value;
  vector<string> &attributeNames;
  RBFM_ScanIterator *rbfm_ScanIterator;
  RID *rid;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) {

    RC rc = rbfm_ScanIterator->getNextRecord(rid, data);
    //data now points to correct attribute data. How does this convert to tuple?
    return rc;
  };

  RC close() {
   return -1; 
  };

};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;


	//file handles and rids used for the catalog /////////////
  	bool catExists;		//catalog exists or not		//
	bool modCat;		//while 0, can't mod cat.	//
	FileHandle tablesFH;	//"Tables" filehandle		//	  
	RID* tTableRID;		//tables table RID		//
	RID* cTableIdRID;	//columns table-id rid		//
	RID* cTableNameRID;	//columns table-name rid	//
	RID* cFileNameRID;	//columns file-name rid 	//
								//
	FileHandle colFH;	//"Columns" filehandle		//	  					  
	RID* tColRID;		//tables Columns RID    	//	  
	RID* cColIdRID;		//columns table-id rid 		//
	RID* cColNameRID;	//columns column-name rid	//
	RID* cColTypeRID;	//columns column-type rid	//
	RID* cColLengthRID;	//columns column-length rid	//
	RID* cColPosRID;	//columns column-position rid	//
	// ///////////////////////////////////////////////////////
};

#endif
