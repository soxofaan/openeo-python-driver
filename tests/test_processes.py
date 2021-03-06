import pytest

from openeo_driver.errors import ProcessUnsupportedException
from openeo_driver.processes import ProcessSpec, ProcessRegistry


def test_process_spec_basic_040():
    spec = (
        ProcessSpec("mean", "Mean value")
            .param("input", "Input data", schema={"type": "array", "items": {"type": "number"}})
            .param("mask", "The mask", schema=ProcessSpec.RASTERCUBE, required=False)
            .returns("Mean value of data", schema={"type": "number"})
    )
    assert spec.to_dict_040() == {
        "id": "mean",
        "description": "Mean value",
        "parameters": {
            "input": {
                "description": "Input data",
                "required": True,
                "schema": {"type": "array", "items": {"type": "number"}}
            },
            "mask": {
                "description": "The mask",
                "required": False,
                "schema": {"type": "object", "format": "raster-cube"}
            }
        },
        "parameter_order": ["input", "mask"],
        "returns": {"description": "Mean value of data", "schema": {"type": "number"}},
    }


def test_process_spec_basic_100():
    spec = (
        ProcessSpec("mean", "Mean value")
            .param("input", "Input data", schema={"type": "array", "items": {"type": "number"}})
            .param("mask", "The mask", schema=ProcessSpec.RASTERCUBE, required=False)
            .returns("Mean value of data", schema={"type": "number"})
    )
    assert spec.to_dict_100() == {
        "id": "mean",
        "description": "Mean value",
        "parameters": [
            {
                "name": "input",
                "description": "Input data",
                "optional": False,
                "schema": {"type": "array", "items": {"type": "number"}}
            },
            {
                "name": "mask",
                "description": "The mask",
                "optional": True,
                "schema": {"type": "object", "format": "raster-cube"}
            }
        ],
        "returns": {"description": "Mean value of data", "schema": {"type": "number"}},
    }


def test_process_spec_no_params_040():
    spec = ProcessSpec("foo", "bar").returns("output", schema={"type": "number"})
    with pytest.warns(UserWarning):
        assert spec.to_dict_040() == {
            "id": "foo", "description": "bar", "parameters": {}, "parameter_order": [],
            "returns": {"description": "output", "schema": {"type": "number"}}
        }


def test_process_spec_no_params_100():
    spec = ProcessSpec("foo", "bar").returns("output", schema={"type": "number"})
    with pytest.warns(UserWarning):
        assert spec.to_dict_100() == {
            "id": "foo", "description": "bar", "parameters": [],
            "returns": {"description": "output", "schema": {"type": "number"}}
        }


def test_process_spec_no_returns():
    spec = ProcessSpec("foo", "bar").param("input", "Input", schema=ProcessSpec.RASTERCUBE)
    with pytest.raises(AssertionError):
        spec.to_dict_100()


def test_process_registry_add_by_name():
    reg = ProcessRegistry()
    reg.add_spec_by_name("max")
    assert set(reg._processes.keys()) == {"max"}
    spec = reg.get_spec('max')
    assert spec['id'] == 'max'
    assert 'largest value' in spec['description']
    assert all(k in spec for k in ['parameters', 'returns'])


def test_process_registry_load_predefined_specs():
    """Test if all spec json files load properly"""
    reg = ProcessRegistry()
    for name in reg.list_predefined_specs().keys():
        spec = reg.load_predefined_spec(name)
        assert spec["id"] == name


def test_process_registry_add_function():
    reg = ProcessRegistry()

    @reg.add_function
    def max(*args):
        return max(*args)

    assert set(reg._processes.keys()) == {"max"}
    spec = reg.get_spec('max')
    assert spec['id'] == 'max'
    assert 'largest value' in spec['description']
    assert all(k in spec for k in ['parameters', 'returns'])

    assert reg.get_function('max') is max


def test_process_registry_with_spec_040():
    reg = ProcessRegistry()

    def add_function_with_spec(spec: ProcessSpec):
        def decorator(f):
            reg.add_function(f=f, spec=spec.to_dict_040())
            return f

        return decorator

    @add_function_with_spec(
        ProcessSpec("foo", "bar")
            .param("input", "Input", schema=ProcessSpec.RASTERCUBE)
            .returns(description="Output", schema=ProcessSpec.RASTERCUBE)
    )
    def foo(*args):
        return 42

    assert reg.get_spec('foo') == {
        "id": "foo",
        "description": "bar",
        "parameters": {
            "input": {"description": "Input", "schema": {"type": "object", "format": "raster-cube"},
                      "required": True},
        },
        "parameter_order": ["input"],
        "returns": {"description": "Output", "schema": {"type": "object", "format": "raster-cube"}}
    }


def test_process_registry_with_spec_100():
    reg = ProcessRegistry()

    def add_function_with_spec(spec: ProcessSpec):
        def decorator(f):
            reg.add_function(f=f, spec=spec.to_dict_100())
            return f

        return decorator

    @add_function_with_spec(
        ProcessSpec("foo", "bar")
            .param("input", "Input", schema=ProcessSpec.RASTERCUBE)
            .returns(description="Output", schema=ProcessSpec.RASTERCUBE)
    )
    def foo(*args):
        return 42

    assert reg.get_spec('foo') == {
        "id": "foo",
        "description": "bar",
        "parameters": [
            {"name": "input", "description": "Input", "schema": {"type": "object", "format": "raster-cube"},
             "optional": False},
        ],
        "returns": {"description": "Output", "schema": {"type": "object", "format": "raster-cube"}}
    }


def test_process_registry_add_deprecated():
    reg = ProcessRegistry()

    @reg.add_deprecated
    def foo(*args):
        return 42

    new_foo = reg.get_function('foo')
    with pytest.warns(UserWarning, match="deprecated process"):
        assert new_foo() == 42
    with pytest.raises(ProcessUnsupportedException):
        reg.get_spec('foo')


def test_process_registry_get_spec():
    reg = ProcessRegistry()
    reg.add_spec_by_name("min")
    reg.add_spec_by_name("max")
    with pytest.raises(ProcessUnsupportedException):
        reg.get_spec('foo')


def test_process_registry_get_specs():
    reg = ProcessRegistry()
    reg.add_spec_by_name("min")
    reg.add_spec_by_name("max")
    reg.add_spec_by_name("sin")
    assert set(p['id'] for p in reg.get_specs()) == {"max", "min", "sin"}
    assert set(p['id'] for p in reg.get_specs('')) == {"max", "min", "sin"}
    assert set(p['id'] for p in reg.get_specs("m")) == {"max", "min"}
    assert set(p['id'] for p in reg.get_specs("in")) == {"min", "sin"}
