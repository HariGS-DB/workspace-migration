from common.registry import OBJECT_TYPES, ObjectTypeConfig, get_types_by_phase


class TestRegistry:
    def test_all_types_have_required_fields(self):
        for name, cfg in OBJECT_TYPES.items():
            assert isinstance(cfg, ObjectTypeConfig), f"{name} is not an ObjectTypeConfig"
            assert isinstance(cfg.worker_notebook, str) and cfg.worker_notebook, f"{name} missing worker_notebook"
            assert isinstance(cfg.phase, str) and cfg.phase, f"{name} missing phase"
            assert isinstance(cfg.batch_size, int) and cfg.batch_size > 0, f"{name} has invalid batch_size"
            assert isinstance(cfg.parallel_threads, int) and cfg.parallel_threads > 0, (
                f"{name} has invalid parallel_threads"
            )

    def test_get_types_by_phase(self):
        phase_1 = get_types_by_phase("parallel_1")
        assert set(phase_1.keys()) == {"managed_table", "external_table", "volume"}

        phase_2 = get_types_by_phase("parallel_2")
        assert set(phase_2.keys()) == {"function"}

        phase_3 = get_types_by_phase("parallel_3")
        assert set(phase_3.keys()) == {"view"}

        phase_4 = get_types_by_phase("parallel_4")
        assert set(phase_4.keys()) == {"grant"}

        empty = get_types_by_phase("nonexistent_phase")
        assert empty == {}

    def test_phases_define_valid_ordering(self):
        """Grants must be last phase; views must come after functions."""
        grant_phase = OBJECT_TYPES["grant"].phase
        view_phase = OBJECT_TYPES["view"].phase
        function_phase = OBJECT_TYPES["function"].phase

        all_phases = sorted({cfg.phase for cfg in OBJECT_TYPES.values()})

        # Grants are in the last phase
        assert grant_phase == all_phases[-1], f"grant phase {grant_phase} is not last among {all_phases}"

        # Views come after functions
        assert view_phase > function_phase, f"view phase {view_phase} should come after function phase {function_phase}"
