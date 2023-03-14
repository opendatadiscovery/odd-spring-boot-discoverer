package org.opendatadiscovery.discoverer.model;

import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Paths {
    private final Set<? extends OddrnPath> inputs;
    private final Set<? extends OddrnPath> outputs;

    public Paths(final Collection<? extends OddrnPath> inputs, final Collection<? extends OddrnPath> outputs) {
        this.inputs = new HashSet<>(inputs);
        this.outputs = new HashSet<>(outputs);
    }

    public Paths(final Set<? extends OddrnPath> inputs, final Set<? extends OddrnPath> outputs) {
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

        return new Paths(inputs, outputs);
    }

    public static Paths empty() {
        return new Paths(Collections.emptySet(), Collections.emptySet());
    }

    public Set<? extends OddrnPath> getInputs() {
        return inputs;
    }

    public Set<? extends OddrnPath> getOutputs() {
        return outputs;
    }
}
