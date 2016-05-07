
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

void* formatData(nullbytes, attributes

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
	recordBasedFileManager rbf; 

	vector<Attribute> tablesAttr;
	vector<Attribute> colAttr;

	const string TABLES_NAME = "Tables";
	const string COL_NAME = "Columns";

	/*these should maybe be private variables in the RM class*/
	/*down to here*******************************************///

	if (exists)
	{
		return (1);	//already exists, can't create again
	}
	
	//create the Tables table
	tablesAttr.resize(3);
	tablesAttr[0].name = "table-id";
	tablesAttr[0].type = TypeInt;
	tablesAttr[0].length = 4;

	tablesAttr[1].name = "table-name";
	tablesAttr[1].type = TypeVarChar;
	tablesAttr[1].length = 50;
	
	tablesAttr[2].name = "file-name";
	tablesAttr[2].type = TypeVarChar;
	tablesAttr[2].length = 50;
	/*use rbf here to create catalog file*/
	rbf.createFile(TABLES_NAME);
	rbf.openFile(TABLES_NAME, tablesFH);
	rbf.insertRecord(tablesFH, tablesAttr, /*some void* data*/, tablesRID);
	//Finish creating the tables table
	
	//create the columns table	
	colAttr.resize(5);
	colAttr[0].name = "table-id";
	colAttr[0].type = TypeInt;
	colAttr[0].length = 4;

	colAttr[1].name = "column-name";
	colAttr[1].type = TypeVarChar;
	colAttr[1].length = 50;
	
	colAttr[2].name = "column-type";
	colAttr[2].type = TypeInt;
	colAttr[2].length = 4;
	
	colAttr[3].name = "column-length";
	colAttr[3].type = TypeInt;
	colAttr[3].length = 4;

	colAttr[4].name = "column-position";
	colAttr[4].type = TypeInt;
	colAttr[4].length = 4;
    	/*use rbf here to create catalog file*/
	rbf.createFile(COL_NAME);
	rbf.openFile(COL_NAME, colFH);
	rbf.insertRecord(colFH, colAttr, /*some void* data*/, colRID);
	//finish creating the columns table
		
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



