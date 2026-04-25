package database

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations applies all pending UP migrations from migrationsPath.
// It is safe to call on every startup — already-applied migrations are skipped.
// Returns an error only if a migration fails; "no change" is treated as success.
func RunMigrations(dsn, migrationsPath string) error {
	sourceURL := "file://" + migrationsPath

	m, err := migrate.New(sourceURL, dsn)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	defer m.Close()

	// Log current version before migrating.
	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("get migration version: %w", err)
	}
	if dirty {
		return fmt.Errorf("database is in dirty state at version %d — manual intervention required", version)
	}

	slog.Info("running database migrations",
		slog.Uint64("current_version", uint64(version)),
		slog.String("source", migrationsPath),
	)

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			slog.Info("database schema is up to date — no migrations applied")
			return nil
		}
		return fmt.Errorf("apply migrations: %w", err)
	}

	newVersion, _, _ := m.Version()
	slog.Info("migrations applied successfully",
		slog.Uint64("new_version", uint64(newVersion)),
	)

	return nil
}