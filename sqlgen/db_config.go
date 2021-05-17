package sqlgen

type ControlOption struct {
	// initial value for state.enableClustered
	InitEnableClustered bool
	// the initial number of tables.
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
	CreateTable                 int
	CreateTable_WithClusterHint bool
	CreateTable_MoreCol         int    // deprecated, use CreateTable_MaxColumnCnt instead.
	CreateTable_WithoutLike     int    // deprecated, it will be removed later.
	CreateTable_Partition_Type  string // deprecated, use self-defined replacer instead.
	CreateTable_IndexMoreCol    int    // deprecated, use self-defined replacer instead.
	CreateTable_MustPrefixIndex bool
	CreateTable_MustStrCol      bool
	CreateTable_MustIntCol      bool
	CreateTable_IgnoredTypeCols []ColumnType
	CreateTable_MaxColumnCnt    int
	Query                       int // deprecated, it will be removed later.
	Query_DML                   int
	Query_Select                int
	Query_DML_DEL               int
	Query_DML_DEL_INDEX         int
	Query_DML_DEL_COMMON        int
	Query_DML_DEL_INDEX_PK      int
	Query_DML_DEL_INDEX_COMMON  int
	Query_DML_INSERT            int
	Query_DML_INSERT_ON_DUP     int // deprecated
	Query_DML_Can_Be_Replace    bool
	Query_DML_UPDATE            int
	Query_DDL                   int
	Query_Window                int
	Query_Union                 int
	Query_Split                 int
	Query_Analyze               int
	Query_Prepare               int
	Query_HasLimit              int // deprecated, use has_order_limit instead.
	Query_HasOrderby            int // deprecated, use has_order_limit instead.
	Query_OrderLimit            string
	Query_INDEX_MERGE           bool
	SetRowFormat                int
	SetClustered                int
	AdminCheck                  int
	MustCTE                     bool
	CTEMultiRatio               int
	CTESimpleSeed               int
	CTEValidSQL                 int
	CTERecursiveDeep            int
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
	CreateTable_MaxColumnCnt:    10,
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
	MustCTE:                     false,
	CTEMultiRatio:               25,
	CTESimpleSeed:               4,
	CTEValidSQL:                 100,
	CTERecursiveDeep:            5,
}
