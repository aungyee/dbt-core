import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dbt.adapters.contracts.catalog_integration import (
    CatalogIntegration as AdapterCatalogIntegration,
)
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SecretRenderer
from dbt_common.clients.system import load_file_contents
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import CompilationError, DbtValidationError


@dataclass
class CatalogIntegration(AdapterCatalogIntegration, dbtClassMixin):
    profile: Optional[str] = None


@dataclass
class Catalog(dbtClassMixin):
    name: str
    # If not specified, write_integration defaults to the integration in integrations if there is only one.
    write_integration: Optional[str] = None
    integrations: List[CatalogIntegration] = field(default_factory=list)

    @classmethod
    def render(
        cls, raw_catalog: Dict[str, Any], renderer: SecretRenderer, default_profile_name: str
    ) -> "Catalog":
        try:
            rendered_catalog = renderer.render_data(raw_catalog)
        except CompilationError:
            # TODO: better error
            raise

        cls.validate(rendered_catalog)

        integrations = []
        for raw_integration in rendered_catalog.get("integrations", []):
            raw_integration["profile"] = raw_integration.get("profile") or default_profile_name

            CatalogIntegration.validate(raw_integration)
            integrations.append(CatalogIntegration.from_dict(raw_integration))

        # Validate + set default write_integration if unset
        write_integration = rendered_catalog.get("write_integration")
        valid_write_integration_names = [integration.name for integration in integrations]
        if write_integration and write_integration not in valid_write_integration_names:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify a 'write_integration' from its set of defined 'integrations': {valid_write_integration_names}. Got: '{write_integration}'."
            )
        elif len(integrations) > 1 and not write_integration:
            raise DbtValidationError(
                f"Catalog '{rendered_catalog['name']}' must specify a 'write_integration' when multiple 'integrations' provided."
            )
        elif not write_integration and len(integrations) == 1:
            write_integration = integrations[0].name

        return cls(
            name=raw_catalog["name"],
            write_integration=write_integration,
            integrations=integrations,
        )


@dataclass
class Catalogs(dbtClassMixin):
    catalogs: List[Catalog]

    @classmethod
    def load(cls, catalog_dir: str, profile: str, cli_vars: Dict[str, Any]) -> "Catalogs":
        catalogs = []

        raw_catalogs = cls._read_catalogs(catalog_dir)

        catalogs_renderer = SecretRenderer(cli_vars)
        for raw_catalog in raw_catalogs.get("catalogs", []):
            catalog = Catalog.render(raw_catalog, catalogs_renderer, profile)
            catalogs.append(catalog)

        return cls(catalogs=catalogs)

    @classmethod
    def _read_catalogs(cls, catalog_dir: str) -> Dict[str, Any]:
        path = os.path.join(catalog_dir, "catalogs.yml")

        contents = None
        if os.path.isfile(path):
            try:
                contents = load_file_contents(path, strip=False)
                yaml_content = load_yaml_text(contents)
                if not yaml_content:
                    # msg = f"The catalogs.yml file at {path} is empty"
                    # TODO: better error
                    raise ValueError
                    # raise DbtProfileError(INVALID_PROFILE_MESSAGE.format(error_string=msg))
                return yaml_content
            # TODO: better error
            except DbtValidationError:
                # msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
                # raise DbtValidationError(msg) from e
                raise

        return {}
