package boost

type Columns map[string]string

type PlanConfig struct {
	IsBoosted bool
	Columns   Columns
	Table     string
}

func NonBoostedPlanConfig() *PlanConfig {
	return &PlanConfig{
		IsBoosted: false,
		Columns:   Columns{},
	}
}

type QueryFilterConfig struct {
	Columns   []string `yaml:"columns"`
	TableName string   `yaml:"tableName"`
}

type QueryFilterConfigs struct {
	BoostConfigs []QueryFilterConfig `yaml:"boostConfigs"`
}
