package boost

import (
	"context"
	"fmt"
	"io"

	"strings"
	"time"

	"vitess.io/vitess/go/cache/redis"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type BoostConfig struct {
	Host         string
	Port         int
	GrpcPort     int
	User         string
	Password     string
	Gtid         *binlogdatapb.VGtid
	Filter       *binlogdatapb.Filter
	RedisColumns []string
	TabletType   topodatapb.TabletType
	Source       string
	Flags        vtgatepb.VStreamFlags
	RedisAddr    string
}

type Boost struct {
	config *BoostConfig
	player *vplayer
}

func NewBoost(port, grpcPort int, host, gtid, filter, columns, tabletType, redisAddr string) (*Boost, error) {
	targetVgtid := &binlogdatapb.VGtid{}
	targetFilter := &binlogdatapb.Filter{}
	if err := json2.Unmarshal([]byte(gtid), &targetVgtid); err != nil {
		log.Error(err)
		return nil, err
	}
	if err := json2.Unmarshal([]byte(filter), &targetFilter); err != nil {
		log.Error(err)
		return nil, err
	}

	cfg := &BoostConfig{
		Host:         host,
		Port:         port,
		GrpcPort:     grpcPort,
		Gtid:         targetVgtid,
		Filter:       targetFilter,
		RedisColumns: strings.Split(columns, ","),
		Flags: vtgatepb.VStreamFlags{
			//MinimizeSkew:      false,
			HeartbeatInterval: 60, //seconds
		},
		RedisAddr: redisAddr,
	}
	switch tabletType {
	case "replica":
		cfg.TabletType = topodatapb.TabletType_REPLICA
	case "rdonly":
		cfg.TabletType = topodatapb.TabletType_RDONLY
	default:
		cfg.TabletType = topodatapb.TabletType_MASTER
	}

	boost := &Boost{
		config: cfg,
	}
	return boost, nil
}

func (b *Boost) Init() error {
	// Create and open the connection pool for dba access.
	dbPool := dbconnpool.NewConnectionPool("dbPool", 10, time.Minute, 0)
	dbPool.Open(
		dbconfigs.New(
			&mysql.ConnParams{
				Host: b.config.Host,
				Port: b.config.Port,
				// Uname : "main",
				// Pass: ,
			},
		),
	)

	cache := redis.NewCacheParams(b.config.RedisAddr, "", 0)

	// for event aplicator
	colInfo, err := buildColInfoMap(context.Background(), dbPool)
	if err != nil {
		log.Error("---- buildColInfoMap ", err)
		return err
	}

	replicatorPlan, err := buildReplicatorPlan(
		b.config.Filter,

		colInfo,
		nil,
		binlogplayer.NewStats(),
		b.config.RedisColumns,
	)
	if err != nil {
		log.Error("---- buildReplicatorPlan", err)
		return err
	}

	b.player = &vplayer{
		currentVgtid:   b.config.Gtid,
		dbPool:         dbPool,
		replicatorPlan: replicatorPlan,
		tablePlans:     make(map[string]*TablePlan),
		cache:          cache,
	}
	return nil
}

func (b *Boost) Close() {
	// close db pool
	b.player.dbPool.Close()
}

func (b *Boost) Play(ctx context.Context) error {
	log.Info("Playing...")
	conn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%v", b.config.Host, b.config.GrpcPort))
	if err != nil {
		log.Errorf("vtgateconn.Dial error: %s", err)
		return err
	}
	defer conn.Close()

	reader, err := conn.VStream(ctx, b.config.TabletType, b.player.currentVgtid, b.config.Filter, &b.config.Flags)
	if err != nil {
		log.Errorf("conn.VStream error: %s", err)
		return err
	}
	connect := false
	numErrors := 0
	for {
		if connect { // if vtgate returns a transient error try reconnecting from the last seen vgtid
			log.Info("Reconnecting... %s %s %s %s", b.config.TabletType, b.player.currentVgtid, b.config.Filter, &b.config.Flags)
			reader, err = conn.VStream(ctx, b.config.TabletType, b.player.currentVgtid, b.config.Filter, &b.config.Flags)
			if err != nil {
				log.Errorf("Reconnecting error: %s", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			connect = false
		}
		e, err := reader.Recv()
		switch err {
		case nil:
			for _, ev := range e {
				if err := b.player.applyVEvent(ctx, ev); err != nil {
					if err != io.EOF {
						log.Errorf("Error applying event: %s", err.Error())
					}
				}
			}
		case io.EOF:
			log.Info("stream ended")
			return nil
		default:
			log.Errorf("%s:: remote error: %v\n", time.Now(), err)
			numErrors++
			if numErrors > 10 { // if vtgate is continuously unavailable error the test
				return fmt.Errorf("too many remote errors")
			}
			if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
				// this should not happen, but maybe the buffering logic might return a transient
				// error during resharding. So adding this logic to reduce future flakiness
				time.Sleep(100 * time.Millisecond)
				connect = true
			} else {
				// failure, stop test
				return fmt.Errorf("failure, stop test")
			}
		}
	}
}
