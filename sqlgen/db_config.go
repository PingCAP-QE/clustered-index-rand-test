package sqlgen

type ControlOption struct {
	// Deprecated. initial value for state.enableClustered
	InitEnableClustered bool
	// Deprecated. the initial number of tables.
	InitTableCount int
	// the number of rows to initialize for each table.
	InitRowCount int
	// the number of Columns for each tables.
	InitColCount int

	// the max number of tables.
	MaxTableNum int
	// for the Columns that have no default value,
	// whether allow to omit column names in 'INSERT' statement.
	StrictTransTable bool
	// indicate that the testing server has gc save point.
	CanReadGCSavePoint bool
	// Test SELECT OUTFILE and LOAD DATA
	EnableSelectOutFileAndLoadData bool
	// Test TiFlash
	EnableTestTiFlash bool
	// Test column type change
	EnableColumnTypeChange bool
	// indicate whether attach stmt inside txn
	AttachToTxn bool
	// max stmt count in a txn
	MaxTxnStmtCount int
	// generate SQL weight
	Weight *Weight
}

type Weight struct {
	CreateTable                 int          // deprecated, use setWeight instead.
	CreateTable_WithClusterHint bool         // deprecated, use state.StoreConfig(ConfigKeyUnitPKNeedClusteredHint, struct{}{}) instead.
	CreateTable_MoreCol         int          // deprecated, use state.SetRepeat(ColumnDefinition, 1, w.CreateTable_MaxColumnCnt) instead.
	CreateTable_WithoutLike     int          // deprecated, use state.SetWeight(CreateTableLike, 0) instead.
	CreateTable_Partition_Type  string       // deprecated, use state.SetWeight(PartitionDefinitionHash, 100) instead.
	CreateTable_IndexMoreCol    int          // deprecated, use state.SetRepeat(IndexDefinition, 1, n) instead.
	CreateTable_MustPrefixIndex bool         // deprecated, use state.StoreConfig(ConfigKeyProbabilityIndexPrefix, 100 * Percent) instead.
	CreateTable_MustStrCol      bool         // deprecated, use state.StoreConfig(ConfigKeyArrayAllowColumnTypes) instead.
	CreateTable_MustIntCol      bool         // deprecated, use state.StoreConfig(ConfigKeyArrayAllowColumnTypes) instead.
	CreateTable_IgnoredTypeCols []ColumnType // deprecated, use state.StoreConfig(ConfigKeyArrayAllowColumnTypes) instead.
	Query                       int          // deprecated, use setWeight instead.
	Query_DML                   int          // deprecated, use setWeight instead.
	Query_Select                int          // deprecated, use setWeight instead.
	Query_DML_DEL               int          // deprecated, use setWeight instead.
	Query_DML_DEL_INDEX         int          // deprecated, use setWeight instead.
	Query_DML_DEL_COMMON        int          // deprecated, use setWeight instead.
	Query_DML_DEL_INDEX_PK      int          // deprecated, use setWeight instead.
	Query_DML_DEL_INDEX_COMMON  int          // deprecated, use setWeight instead.
	Query_DML_INSERT            int          // deprecated, use setWeight instead.
	Query_DML_INSERT_ON_DUP     int          // deprecated, use setWeight instead.
	Query_DML_Can_Be_Replace    bool         // deprecated, use state.StoreConfig(ConfigKeyEnumInsertOrReplace) instead.
	Query_DML_UPDATE            int          // deprecated, use setWeight instead.
	Query_DDL                   int          // deprecated, use setWeight instead.
	Query_Window                int          // deprecated, use setWeight instead.
	Query_Union                 int          // deprecated, use setWeight instead.
	Query_Split                 int          // deprecated, use setWeight instead.
	Query_Analyze               int          // deprecated, use setWeight instead.
	Query_Prepare               int          // deprecated, use setWeight instead.
	Query_HasLimit              int          // deprecated, use state.StoreConfig(ConfigKeyEnumLimitOrderBy) instead.
	Query_HasOrderby            int          // deprecated, use state.StoreConfig(ConfigKeyEnumLimitOrderBy) instead.
	Query_OrderLimit            string       // deprecated, use state.StoreConfig(ConfigKeyEnumLimitOrderBy) instead.
	Query_INDEX_MERGE           bool
	SetRowFormat                int // deprecated, use setWeight instead.
	SetClustered                int // deprecated, use setWeight instead.
	AdminCheck                  int // deprecated, use setWeight instead.
}

func DefaultControlOption() *ControlOption {
	cloneWeight := DefaultWeight
	return &ControlOption{
		InitEnableClustered:            true,
		InitTableCount:                 5,
		InitRowCount:                   10,
		InitColCount:                   5,
		MaxTableNum:                    7,
		StrictTransTable:               true,
		CanReadGCSavePoint:             false,
		EnableSelectOutFileAndLoadData: false,
		EnableTestTiFlash:              false,
		EnableColumnTypeChange:         true,
		AttachToTxn:                    false,
		MaxTxnStmtCount:                20,
		Weight:                         &cloneWeight,
	}
}

var DefaultWeight = Weight{
	CreateTable:                 13,
	CreateTable_WithClusterHint: true,
	CreateTable_MoreCol:         2,
	CreateTable_IndexMoreCol:    4,
	CreateTable_MustPrefixIndex: false,
	CreateTable_WithoutLike:     4,
	CreateTable_Partition_Type:  "",
	CreateTable_MustStrCol:      false,
	CreateTable_MustIntCol:      false,
	Query:                       15,
	Query_DML:                   20,
	Query_Select:                1,
	Query_DML_DEL:               1,
	Query_DML_DEL_INDEX:         0,
	Query_DML_DEL_COMMON:        1,
	Query_DML_DEL_INDEX_PK:      1,
	Query_DML_DEL_INDEX_COMMON:  1,
	Query_DML_UPDATE:            1,
	Query_Union:                 1,
	Query_DML_INSERT:            1,
	Query_DML_INSERT_ON_DUP:     4,
	Query_Window:                1,
	Query_DML_Can_Be_Replace:    true,
	Query_DDL:                   5,
	Query_Split:                 1,
	Query_Analyze:               0,
	Query_Prepare:               2,
	Query_HasLimit:              0,
	Query_HasOrderby:            0,
	Query_OrderLimit:            "none",
	SetClustered:                1,
	SetRowFormat:                1,
	AdminCheck:                  1,
}
