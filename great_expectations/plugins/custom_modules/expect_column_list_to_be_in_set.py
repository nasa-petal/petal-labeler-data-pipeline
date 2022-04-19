from great_expectations.core import ExpectationConfiguration, ExpectationValidationResult
from great_expectations.execution_engine import (
   ExecutionEngine,
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import InvalidExpectationConfigurationError, ColumnMapExpectation
import great_expectations.expectations.metrics
from great_expectations.expectations.metrics import (
   ColumnMapMetricProvider,
   ColumnAggregateMetricProvider,
   ColumnValuesUnique,
   ColumnUniqueProportion,
   column_aggregate_value,
   column_aggregate_partial,
   column_condition_partial
   
)

import great_expectations.exceptions
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent, RenderedGraphContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

from typing import Any, Dict, List, Optional, Union
_VALUE_SET = set([
          "attach",
          "modify_color/camouflage",
          "modify_size/shape/material_properties",
          "modify/convert_energy",
          "assemble/break_down_structure",
          "move_on/through_solids_liquids_gases",
          "protect_from_living/non-living_threats",
          "manage_mechanical_forces",
          "sustain_ecological_community",
          "chemically_assemble/break_down",
          "sense_send_process_information",
          "manipulate_solids_liquids_gases_energy"
        ])

class ColumnBadListProportionCount(ColumnAggregateMetricProvider):
    metric_name = "column_values.list_in_set.custom.unexpected_count"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):[]
        "Pandas Lists with Bad Value Count"
        bad_lists = column[~column.apply(lambda el: _VALUE_SET.issuperset(el))]
        return bad_lists.shape[0]


class ColumnBadListValues(ColumnAggregateMetricProvider):
    metric_name = "column_values.list_in_set.custom.unexpected_values"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        "Pandas Non-List Values"
        bad_lists = column[~column.apply(lambda el: _VALUE_SET.issuperset(el))] \
            .apply(lambda el: str(el))
        return bad_lists.to_list()

class ExpectColumnListToBeInSet(ColumnMapExpectation):
    map_metric = "column_values.list_in_set.custom"
    success_keys = ("min_value", "strict_min", "max_value", "strict_max","value_set")

    # Default values
    success_keys = ("mostly",)

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "mostly", "row_condition", "condition_parser","value_set"],
        )

        if include_column_name:
            template_str = "$column list values must be in expected set"
        else:
            template_str = "list values must be in expected set"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]
