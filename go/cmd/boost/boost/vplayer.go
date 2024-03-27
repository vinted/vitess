package boost

import (
	"context"
	"fmt"
	"strings"

	"vitess.io/vitess/go/cache/redis"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// vplayer replays binlog events by pulling them from a vstreamer.
type vplayer struct {
	replicatorPlan *ReplicatorPlan
	tablePlans     map[string]*TablePlan

	currentVgtid *binlogdatapb.VGtid
	// fields       []*querypb.Field
	dbPool *dbconnpool.ConnectionPool
	cache  *redis.Cache
}

func (vp *vplayer) applyVEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	switch event.Type {
	case binlogdatapb.VEventType_GTID:
		vp.currentVgtid = event.Vgtid
	case binlogdatapb.VEventType_FIELD:
		b := []string{}
		for _, f := range event.FieldEvent.Fields {
			b = append(b, fmt.Sprintf("%q:%q", f.Name, f.Type))
		}
		log.Infof("field event: { \"table\" : %q, \"fields\": { %s } }", event.FieldEvent.TableName, strings.Join(b[:], ","))
		// --
		tplan, err := vp.replicatorPlan.buildExecutionPlan(event.FieldEvent)
		if err != nil {
			return err
		}

		vp.tablePlans[event.FieldEvent.TableName] = tplan
		log.Info("new table plan")
	case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE,
		binlogdatapb.VEventType_REPLACE, binlogdatapb.VEventType_SAVEPOINT:
		return fmt.Errorf("not implemented event types: INSERT,DELETE,REPLACE,UPDATE,SAVEPOINT")
	case binlogdatapb.VEventType_ROW:
		if err := vp.applyRowEvent(ctx, event.RowEvent); err != nil {
			return err
		}

		// //Row event is logged AFTER RowChanges are applied so as to calculate the total elapsed time for the Row event
		// stats.Send(fmt.Sprintf("%v", event.RowEvent))
	}
	return nil
}

// applyRowEvent applies mysql events
// example sql generated:
//   - insert into items(id,user_id,`desc`,`status`) values (3924,5,null,0)
//   - update items set user_id=999, `desc`=null, `status`=0 where id=3924
//   - delete from items where id=3924
func (vp *vplayer) applyRowEvent(ctx context.Context, rowEvent *binlogdatapb.RowEvent) error {
	tplan := vp.tablePlans[rowEvent.TableName]
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", rowEvent.TableName)
	}
	for _, change := range rowEvent.RowChanges {
		_, err := tplan.applyRedisChange(change, func(key string) (*sqltypes.Result, error) {
			// this one for Mysql change
			// _, err := tplan.applyChange(change, func(sql string) (*sqltypes.Result, error) {
			// stats := NewVrLogStats("ROWCHANGE")
			// start := time.Now()
			log.Infof("Deleting key: %s", key)
			err := vp.cache.Delete(key)
			// fmt.Println(sql)
			return nil, err
			// qr, err := vp.dbClient.ExecuteWithRetry(ctx, sql)
			// vp.vr.stats.QueryCount.Add(vp.phase, 1)
			// vp.vr.stats.QueryTimings.Record(vp.phase, start)
			// stats.Send(sql)
			// return qr, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
