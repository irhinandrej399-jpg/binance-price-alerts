// Package sourcebinding contains SQL provenance decoding shared by matcher stores.
package sourcebinding

import (
	"errors"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrUnbound = errors.New("matcher source: legacy state has no proven provenance")

func Error(err error) error {
	var e *pgconn.PgError
	if errors.As(err, &e) {
		switch e.Code {
		case "P0610":
			return errors.Join(ErrUnbound, err)
		case "P0611":
			return errors.Join(kafkasource.ErrMismatch, err)
		}
	}
	return err
}

// Decode preserves NULL as unbound; it never fills provenance from config.
func Decode(cluster *string, tid []byte, topic *string) (*kafkasource.Identity, error) {
	if cluster == nil && tid == nil {
		return nil, nil
	}
	if cluster == nil || topic == nil || len(tid) != 16 {
		return nil, kafkasource.ErrIdentity
	}
	id := kafkasource.Identity{ClusterID: *cluster, Topic: *topic}
	copy(id.TopicID[:], tid)
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return &id, nil
}
func Check(expected *kafkasource.Identity, actual *kafkasource.Identity, hasCursor bool) error {
	if expected == nil {
		return nil
	}
	if err := expected.Validate(); err != nil {
		return err
	}
	if actual == nil {
		if hasCursor {
			return ErrUnbound
		}
		return nil
	}
	if *actual != *expected {
		return kafkasource.ErrMismatch
	}
	return nil
}
