
#include "rm.h"
#include <stdlib.h>

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
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance(); 

	vector<Attribute> tablesAttr;
	vector<Attribute> colAttr;

	const string TABLES_NAME = "Tables";
	const string COL_NAME = "Columns";

	
	if (exists)
	{
		return (1);	//already exists, can't create again
	}
	
	void* data = malloc(1024);
	char offset = 0;
	
	//create the Tables table	
	int tableID;
	int stringLength;

	tablesAttr.resize(3);
	tablesAttr[0] = setAttr("table-id", TypeInt, 4);
	tablesAttr[1] = setAttr("table-name", TypeVarChar, 50);
	tablesAttr[2] = setAttr("file-name", TypeVarChar, 50);
	
	/*use rbf here to create catalog file*/
	rbf->createFile(TABLES_NAME);
	rbf->openFile(TABLES_NAME, tablesFH);
	

		
	*(char*)data = 0;				//why did I subject myself to this?
	offset = offset + 1;
	tableID = 1;
	*(int*)((char*)data + offset)= tableID;
	offset = offset + 4;
	stringLength = 6;	
	*(int*)((char*)data + offset)= stringLength;
	offset = offset + 4;	
	*((char*)data + offset)= TABLES_NAME;
	offset = offset + 6;
	*(int*)((char*)data + offset)= stringLength;
	offset = offset + 4;	
	*((char*)data + offset)= TABLES_NAME;
	offset = offset + 6;

	rbf.insertRecord(tablesFH, tablesAttr, data, tablesTableRID);
	
	//add columns table to tables table
	offset = 0;
	*(char*)data = 0;				//why did I subject myself to this again?
	offset = offset + 1;
	tableID = 1;
	*(int*)((char*)data + offset)= tableID;
	offset = offset + 4;
	stringLength = 6;	
	*(int*)((char*)data + offset)= stringLength;
	offset = offset + 4;	
	*((char*)data + offset)= COL_NAME;
	offset = offset + 6;
	*(int*)((char*)data + offset)= stringLength;
	offset = offset + 4;	
	*((char*)data + offset)= COL_NAME;
	offset = offset + 6;

	rbf.insertRecord(tablesFH, tablesAttr, data, colTableRID);
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
	//rbf.insertRecord(colFH, colAttr, /*some void* data*/, colRID);
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
    RC err;
    if(this->createCatalog() == -1)
        return -1;
    // Scan using the lower level rbfm scanner. 
    RBFM_Iterator iterator;
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if(rbfm->openFile("Tables", fh) == -1)
        return -1;
    if(rbfm->scan(fh, ))
    

    this->scan("Tables", "table-name", EQ_OP, (void *)tableName, )
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->insertRecord(fh, attrs, data, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->deleteRecord(fh, attrs, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->updateRecord(fh, attrs, data, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->readRecord(fh, attrs, rid, data);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	return rbfm->printAttributes(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->updateRecord(fh, attrs, rid, attributeName, data);
    if(err == -1)
        return err;
    return 0;
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
