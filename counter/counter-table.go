package counter

const TableNameCounter = "counters"

// Attribute mapped from table <attributes>
type CounterRecord struct {
	PersistenceId  string `gorm:"column:persistence_id;primaryKey" json:"persistence_id"`
	SequenceNumber int64  `gorm:"column:sequence_number;not null" json:"sequence_number"`
	Timestamp      int64  `gorm:"column:timestamp;not null" json:"timestamp"`
	Counter        int    `gorm:"column:counter;not null" json:"counter"`
}

// TableName Attribute's table name
func (*CounterRecord) TableName() string {
	return TableNameCounter
}
