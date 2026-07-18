package app

// The v2 network starts from a clean genesis, so no historical v0.2.x upgrade handlers
// or module-version migrations apply. Future upgrades register their handlers here.
func (app *App) setupUpgradeHandlers() {}

func (app *App) registerMigrations() {}
