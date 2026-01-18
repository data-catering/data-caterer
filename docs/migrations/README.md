# Data Caterer Migrations

This directory contains migration guides and tools for upgrading between Data Caterer versions and configuration formats.

## Available Migrations

### [YAML Unified Format Migration](yaml-unified-format/)

**From:** Legacy YAML format (separate plan + task files)
**To:** Unified YAML format v1.0 (single-file configuration)
**Status:** ✅ Production Ready
**Data Caterer Version:** v1.0+

Migrates your existing YAML plans to the new unified format that simplifies configuration management.

**Quick Start:**
```bash
cd docs/migrations/yaml-unified-format
python3 migrate_yaml.py <input_plan.yaml> [output_plan.yaml]
```

**Documentation:** [MIGRATION.md](yaml-unified-format/MIGRATION.md)

---

## Migration Best Practices

### Before You Migrate

1. **Backup your configuration files**
   ```bash
   cp -r /path/to/plans /path/to/plans.backup
   ```

2. **Review the migration guide** for your specific migration
3. **Test with dry-run** mode if available
4. **Validate migrated files** before deploying to production

### Testing Migrations

Always test your migrated configuration before deploying:

```bash
# For YAML configurations
PLAN_FILE_PATH=/path/to/migrated_plan.yaml ./gradlew :app:run

# Or use manual tests
YAML_FILE=/path/to/migrated_plan.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
```

### Getting Help

- **Documentation:** Check the specific migration folder for detailed guides
- **Examples:** Look for `examples/` subdirectories in migration folders
- **Issues:** Report problems at [GitHub Issues](https://github.com/data-catering/data-caterer/issues)
- **Tag your issue:** Use `migration:<migration-name>` label

---

## Future Migrations

As Data Caterer evolves, additional migrations will be added here:

- Schema versioning migrations
- Configuration format changes
- API breaking changes
- Database schema updates

Each migration will have its own subdirectory with:
- Migration guide (MIGRATION.md)
- Automated tools (scripts, utilities)
- Test suite
- Examples

---

## Contributing Migration Tools

If you develop migration tools or scripts, consider contributing:

1. Create a subdirectory: `docs/migrations/<migration-name>/`
2. Include:
   - `MIGRATION.md` - Comprehensive guide
   - Migration tool/script
   - Test suite
   - Example files
3. Update this README with your migration
4. Submit a pull request

---

**Last Updated:** 2026-01-12
**Maintained By:** Data Caterer Team
