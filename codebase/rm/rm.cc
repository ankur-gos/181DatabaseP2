#include "../rbf/rbfm.h"
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
void catTDSetup(void* data, int tableID, string tableName);
void catCDSetup(void* data, int tableID, string colName, int colType, int colLength, int colPos);

RelationManager::RelationManager()
{
  	catExists = 0;
	nextTableID = 1;

	tTableRID 	= NULL;
	cTableIdRID	= NULL;
	cTableNameRID	= NULL;
	cFileNameRID 	= NULL;
	
	tColRID		= NULL;	  
	cColIdRID	= NULL;
	cColNameRID	= NULL;	
	cColTypeRID	= NULL;
	cColLengthRID	= NULL;
	cColPosRID	= NULL;		
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
	if (catExists)
	{
		return (1);	//already exists, can't create again
	}

	modCat = 1;

	tTableRID 	= (RID*)malloc(sizeof(RID));
	cTableIdRID	= (RID*)malloc(sizeof(RID));
	cTableNameRID	= (RID*)malloc(sizeof(RID));
	cFileNameRID 	= (RID*)malloc(sizeof(RID));
	
	tColRID		= (RID*)malloc(sizeof(RID)); 
	cColIdRID	= (RID*)malloc(sizeof(RID));
	cColNameRID	= (RID*)malloc(sizeof(RID));	
	cColTypeRID	= (RID*)malloc(sizeof(RID));
	cColLengthRID	= (RID*)malloc(sizeof(RID));
	cColPosRID	= (RID*)malloc(sizeof(RID));	


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
	nextTableID++;	

	catTDSetup(data, 1, TABLES_NAME);
	rbf->insertRecord(tablesFH, tablesAttr, data, *tTableRID);
	
	//add columns table to tables table
	catTDSetup(data, 2, COL_NAME);
	rbf->insertRecord(tablesFH, tablesAttr, data, *tColRID);
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
	nextTableID++;	

	//add the columns of "Tables" and "Columns" to "Columns"	
	catCDSetup(data, 1, "table-id", TypeInt, 4, 1);
	rbf->insertRecord(colFH, colAttr, data, *cTableIdRID);
	
	catCDSetup(data, 1, "table-name", TypeVarChar, 50, 2);
	rbf->insertRecord(colFH, colAttr, data, *cTableNameRID);
	
	catCDSetup(data, 1, "file-name", TypeVarChar, 50, 3);
	rbf->insertRecord(colFH, colAttr, data, *cFileNameRID);
	

	catCDSetup(data, 2, "table-id", TypeInt, 4, 1);
	rbf->insertRecord(colFH, colAttr, data, *cColIdRID);
	
	catCDSetup(data, 2, "column-name", TypeVarChar, 50, 2);
	rbf->insertRecord(colFH, colAttr, data, *cColNameRID);
	
	catCDSetup(data, 2, "column-type", TypeInt, 4, 3);
	rbf->insertRecord(colFH, colAttr, data, *cColTypeRID);
	
	catCDSetup(data, 2, "column-length", TypeInt, 4, 4);
	rbf->insertRecord(colFH, colAttr, data, *cColLengthRID);
	
	catCDSetup(data, 2, "column-position", TypeInt, 4, 5);
	rbf->insertRecord(colFH, colAttr, data, *cColPosRID);
	//finish creating the columns table
	

	free(data);
	catExists = 1;
	modCat = 0;
	return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager* rbf = RecordBasedFileManager::instance();

	catExists = 0;	//begin by saying the catalog is gone
	modCat = 1;
	
	//close the filehandles
	rbf->closeFile(colFH);	
	rbf->closeFile(tablesFH);

	//destroy the files
	rbf->destroyFile("Columns");
	rbf->destroyFile("Tables");	

	//free all of the catalog RIDs
	free(tTableRID);
	free(cTableIdRID);
	free(cTableNameRID);
	free(cFileNameRID);
	
	free(tColRID);	  
	free(cColIdRID);
	free(cColNameRID);	
	free(cColTypeRID);
	free(cColLengthRID);
	free(cColPosRID);		


	tTableRID 	= NULL;
	cTableIdRID	= NULL;
	cTableNameRID	= NULL;
	cFileNameRID 	= NULL;
	
	tColRID		= NULL;	  
	cColIdRID	= NULL;
	cColNameRID	= NULL;	
	cColTypeRID	= NULL;
	cColLengthRID	= NULL;
	cColPosRID	= NULL;	

	modCat = 0;	
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	FileHandle fh;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();

	void* data = malloc(1024);
	void* value = malloc(50+1);
	RID throwawayRID;

	RM_ScanIterator rmsi;
	vector<string> tableID = {"table-id"};
	strcpy((char*)value, tableName.c_str());
	scan("Tables", "table-name", EQ_OP, value, tableID, rmsi);	
    	if(rmsi.getNextTuple(throwawayRID, data) != RM_EOF)
	{
		return 1;	//return 1 if there's already a table with that name
	}

	rbf->createFile(tableName);	
	
	//insert into table into "Tables"
	catTDSetup(data, nextTableID, tableName); 
	modCat = 1;
	insertTuple("Tables", data, throwawayRID);
	modCat = 0;
	//insert columns into "Columns"
	for(int i = 0; i<attrs.size(); i++)
	{
		catCDSetup( data, nextTableID, attrs[i].name, attrs[i].type, attrs[i].length, i+1);
		modCat = 1;
		insertTuple("Columns", data, throwawayRID);
		modCat = 0;
	}
		
	free(data);
	free(value);
	nextTableID++;
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{	
	RID rid;
	RM_ScanIterator rmsi;
	vector<string> table = {"table-id", "file-name"};
	void* data = malloc(1024);
	void* value = malloc(50+1);
	string fileName;

	strcpy((char*) value, tableName.c_str());
	//delete entries in Tables
	modCat = 1;
	scan("Tables", "table-name", EQ_OP, value, table, rmsi);	
    	while(rmsi.getNextTuple(rid, data) != RM_EOF)
	{
		*(int*)value = *(int*)((char*)data+1);
		deleteTuple(tableName, rid);
		int length = *(int*)((char*)data+5);
		char* fName = (char*)malloc(50 + 1);
		memcpy((void*)fName, (void*)((char*)data+9), length);
		*(fName+length) = '\0';
		fileName = fName;
		free(fName);
	}

	RecordBasedFileManager* rbf = RecordBasedFileManager::instance();
	rbf->destroyFile(fileName);
	
	vector<string> columns = {"table-id"};
	//Delete Entries in columns
	scan("Columns", "table-id", EQ_OP, value, columns, rmsi);	
    	while(rmsi.getNextTuple(rid, data) != RM_EOF)
	{
		deleteTuple(tableName, rid);
	}
	modCat = 0;	

	free(data);	
	free(value);
	return 0;
}

vector<Attribute> getCatalogTableAttributes(){
    vector<Attribute> attrs;
    Attribute a = {"table-id", TypeInt, 4};
    Attribute b = {"table-name", TypeVarChar, 50};
    Attribute c = {"file-name", TypeVarChar, 50};
    attrs.push_back(a);
    attrs.push_back(b);
    attrs.push_back(c);
    return attrs;
}

vector<Attribute> getCatalogColumnAttributes(){
    vector<Attribute> attrs;
    Attribute a = {"table-id", TypeInt, 4};
    Attribute b = {"column-name", TypeVarChar, 50};
    Attribute c = {"column-type", TypeInt, 4};
    Attribute d = {"column-length", TypeInt, 4};
    Attribute e = {"column-position", TypeInt, 4};
    attrs.push_back(a);
    attrs.push_back(b);
    attrs.push_back(c);
    attrs.push_back(d);
    attrs.push_back(e);
    return attrs;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if(this->createCatalog() == -1)
        return -1;
    // Scan using the lower level rbfm scanner. 
    RBFM_ScanIterator iterator;
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if(rbfm->openFile("Tables", fh) == -1)
        return -1;

    vector<string> s;
    s.push_back("table-id");
    printf("tableName: %s\n", tableName.c_str());
    if(rbfm->scan(fh, getCatalogTableAttributes(), "table-name", EQ_OP, (const void *)tableName.c_str(), s, iterator) == -1)
        return -1;
    RID rid;
    void *data = malloc(5);
    if(iterator.getNextRecord(rid, data) == -2)
        return -1;
    printf("%d\n", *(int *)data);
    int *id = (int *)((char *)data + 1);
    if(rbfm->openFile("Columns", fh) == -1)
        return -1;
    vector<string> colAttributes;
    colAttributes.push_back("column-name");
    colAttributes.push_back("column-type");
    colAttributes.push_back("column-length");
    colAttributes.push_back("column-position");
    if(rbfm->scan(fh, getCatalogColumnAttributes(), "table-id", EQ_OP, (void *)id, colAttributes, iterator) == -1)
        return -1;
    // 67 is number of bytes per row in catalog column
    void *colData = malloc(67);
    size_t offset = 1;
    vector<Attribute> attributes;
    while(iterator.getNextRecord(rid, colData) != RBFM_EOF){
        int *nameLength = (int *)((char *)colData + offset);
        offset += 4;
        char *str = (char *)malloc(*nameLength + 1);
        memcpy((void *)str, (void *)((char *)colData + offset), *nameLength);
        *(str + *nameLength) = '\0';
        string columnName = str;
        free(str);
        offset += *nameLength;
        int *colType = (int *)((char *)colData + offset);
        offset += 4;
        AttrLength *colLength = (AttrLength *)((char *)colData + offset);
        offset += 4;
        // int *colPos = (int *)((char *)colData + offset);
        Attribute a = {columnName, (AttrType)*colType, *colLength};
        // printf("colName: %s\n", columnName.c_str());
        // printf("nameLength: %d\n", *nameLength);
        // printf("colLength: %d\n", *colLength);
        attributes.push_back(a);
    }
    free(data);
    free(colData);
    attrs = attributes;
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if(this->modCat != 1 && (tableName == "Tables" || tableName == "Columns"))
        return -1;

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
    if(this->modCat != 1 && (tableName == "Tables" || tableName == "Columns"))
        return -1;
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
    if(this->modCat != 1 && (tableName == "Tables" || tableName == "Columns"))
        return -1;
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
	return rbfm->printRecord(attrs, data);
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
    err = rbfm->readAttribute(fh, attrs, rid, attributeName, data);
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
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if(rbfm->openFile(tableName, fh) == -1)
        return -1;
    vector<Attribute> attr;
    if(this->getAttributes(tableName, attr) == -1)
        return -1;
    RBFM_ScanIterator iterator;
    if(rbfm->scan(fh, attr, conditionAttribute, compOp, value, attributeNames, iterator) == -1)
        return -1;
    rm_ScanIterator.iterator = &iterator;
    return 0;
}



Attribute setAttr(string name, AttrType type, AttrLength length)
{
	Attribute attr;
	attr.name = name;
	attr.type = type;
	attr.length = length;
	return (attr);
} 
