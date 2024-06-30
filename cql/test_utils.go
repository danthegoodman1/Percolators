package cql

import (
	"fmt"
	"github.com/gocql/gocql"
)

func createKeyspace(s *gocql.Session) error {
	if err := s.Query("DROP KEYSPACE if exists test").Exec(); err != nil {
		return fmt.Errorf("error dropping keyspace: %w", err)
	}

	if err := s.Query("CREATE KEYSPACE if not exists test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 1}").Exec(); err != nil {
		return fmt.Errorf("error creating keyspace: %w", err)
	}

	return nil
}

func createTable(s *gocql.Session) error {
	if err := s.Query("create table if not exists testkv (key text, col text, ts bigint, val blob, primary key (key, col, ts)) with clustering order by (col desc, ts desc)").Exec(); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	return nil
}

func setupTest() (*gocql.Session, error) {
	cluster := gocql.NewCluster("localhost:9042")
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session: %w", err)
	}

	err = createKeyspace(s)
	if err != nil {
		return nil, fmt.Errorf("error creating keyspace: %w", err)
	}

	cluster.Keyspace = "test"

	s, err = cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session again: %w", err)
	}

	err = createTable(s)
	if err != nil {
		return nil, fmt.Errorf("error creating table: %w", err)
	}

	return s, nil
}
