package org.opendatadiscovery.discoverer.model;

import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Paths {
    private final List<? extends OddrnPath> inputs;
    private final List<? extends OddrnPath> outputs;

    public Paths(final List<? extends OddrnPath> inputs, final List<? extends OddrnPath> outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public static Paths merge(final List<Paths> paths) {
        final Set<OddrnPath> inputs = new HashSet<>();
        final Set<OddrnPath> outputs = new HashSet<>();

        for (final Paths path : paths) {
            if (path == null) {
                continue;
            }

            if (path.getInputs() != null && !path.getInputs().isEmpty()) {
                inputs.addAll(path.getInputs());
            }

            if (path.getOutputs() != null && !path.getOutputs().isEmpty()) {
                outputs.addAll(path.getOutputs());
            }
        }

        return new Paths(new ArrayList<>(inputs), new ArrayList<>(outputs));
    }

    public List<? extends OddrnPath> getInputs() {
        return inputs;
    }

    public List<? extends OddrnPath> getOutputs() {
        return outputs;
    }
}
