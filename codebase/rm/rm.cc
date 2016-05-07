
#include "rm.h"
#include <stdlib.h>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

Attribute setAttr(string name, AttrType type, AttrLength length); 

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

void catTDSetup(void* data, int tableID, string tableName) 
{
	int stringLength = 0;
	int offset = 0;
	*(char*)data = 0;				//why did I subject myself to this?
	offset = offset + 1;	//offset by nullByte
	*(int*)((char*)data + offset)= tableID;	//add tableID int to data
	offset = offset + 4;	//offset by sizeof tableID
	stringLength = tableName.length();	//length of the name is 6 bytes
	*(int*)((char*)data + offset)= stringLength;	//add the length of the string to data
	offset = offset + 4;	//offset by length of the length of the string
	strcpy(((char*)data + offset), tableName.c_str());	//add string to data (table name)
	offset = offset + stringLength;	//offset by length of the string
	*(int*)((char*)data + offset)= stringLength;	//add length of the string to data
	offset = offset + 4;	//offset by length of the length of the string
	strcpy(((char*)data + offset), tableName.c_str());	//add string to data
	offset = offset + stringLength;	//offset by length of the string
}


void catCDSetup(void* data, int tableID, string colName, int colType, int colLength, int colPos)
{
	int stringLength = 0;
	int offset = 0;
	*(char*)data = 0;		//add nullbyte, no null fields
	offset = offset + 1;		//offset by null bye
	*(int*)((char*)data + offset)= tableID;	//add table id to the table
	offset = offset + 4;	//offset by size of table id (4 bytes)
	stringLength = colName.length();	//size of column-name field "table-id"
	*(int*)((char*)data + offset)= stringLength;	//add length of the string to data
	offset = offset + 4;	//offset by length of the length of the string
	strcpy(((char*)data + offset), colName.c_str());	//add string to data "table-id"
	offset = offset + stringLength;	//offset by length of the string
	*(int*)((char*)data + offset) = colType;
	offset = offset + 4;	//offset by column-type	
	*(int*)((char*)data + offset) = colLength;
	offset = offset + 4;	//offset by column-length	
	*(int*)((char*)data + offset) = colPos;
	offset = offset + 4;	//offset by column-position
} 
/*Notes about the contents of the catalog
	The catalog contains 2 tables:
		Tables
			table-id	int
			table-name	varChar(50)
			file-name	varChar(50)	
				can use our own naming convention for file-name
		Columns
			table-id	int	foreign key
			column-name	varChar(50)
			column-type	int	(enumerated values for int/char/varChar)
			column-length	int
			column-position	int
		table-id and column position both begin at 1, not 0.
	if catalog already exists, return error

	struct attribute contains
		string name
		attrType type
		attrLength length
*/
RC RelationManager::createCatalog()
{
	static bool exists = 0;


	if (exists)
	{
		return (1);	//already exists, can't create again
	}


	RecordBasedFileManager *rbf = RecordBasedFileManager::instance(); 

	vector<Attribute> tablesAttr;
	vector<Attribute> colAttr;

	const string TABLES_NAME = "Tables";
	const string COL_NAME = "Columns";	
	
	void* data = malloc(1024);
	
	//create the Tables table	

	tablesAttr.resize(3);
	tablesAttr[0] = setAttr("table-id", TypeInt, 4);
	tablesAttr[1] = setAttr("table-name", TypeVarChar, 50);
	tablesAttr[2] = setAttr("file-name", TypeVarChar, 50);
	
	/*use rbf here to create catalog file*/
	rbf->createFile(TABLES_NAME);
	rbf->openFile(TABLES_NAME, tablesFH);
	
	catTDSetup(data, 1, TABLES_NAME);
	rbf->insertRecord(tablesFH, tablesAttr, data, tTableRID);
	
	//add columns table to tables table
	catTDSetup(data, 2, COL_NAME);
	rbf->insertRecord(tablesFH, tablesAttr, data, tColRID);
	//Finish creating the tables table
	
	//create the columns table	
	colAttr.resize(5);
	colAttr[0] = setAttr("table-id", TypeInt, 4);
	colAttr[1] = setAttr("column-name", TypeVarChar, 50);	
	colAttr[2] = setAttr("column-type", TypeInt, 4);
	colAttr[3] = setAttr("column-length", TypeInt, 4);
	colAttr[4] = setAttr("column-position", TypeInt, 4);
    	/*use rbf here to create catalog file*/
	rbf->createFile(COL_NAME);
	rbf->openFile(COL_NAME, colFH);
	
	
	catCDSetup(data, 1, "table-id", TypeInt, 4, 1);
	rbf->insertRecord(colFH, colAttr, data, cTableIdRID);
	
	catCDSetup(data, 1, "table-name", TypeVarChar, 50, 2);
	rbf->insertRecord(colFH, colAttr, data, cTableNameRID);
	
	catCDSetup(data, 1, "file-name", TypeVarChar, 50, 3);
	rbf->insertRecord(colFH, colAttr, data, cFileNameRID);
	

	catCDSetup(data, 2, "table-id", TypeInt, 4, 1);
	rbf->insertRecord(colFH, colAttr, data, cColIdRID);
	
	catCDSetup(data, 2, "column-name", TypeVarChar, 50, 2);
	rbf->insertRecord(colFH, colAttr, data, cColNameRID);
	
	catCDSetup(data, 2, "column-type", TypeInt, 4, 3);
	rbf->insertRecord(colFH, colAttr, data, cColTypeRID);
	
	catCDSetup(data, 2, "column-length", TypeInt, 4, 4);
	rbf->insertRecord(colFH, colAttr, data, cColLengthRID);
	
	catCDSetup(data, 2, "column-position", TypeInt, 4, 5);
	rbf->insertRecord(colFH, colAttr, data, cColPosRID);
	//finish creating the columns table
	

	free(data);
	exists = 1;
	return -1;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}



Attribute setAttr(string name, AttrType type, AttrLength length)
{
	Attribute attr;
	attr.name = name;
	attr.type = type;
	attr.length = length;
	return (attr);
} 
