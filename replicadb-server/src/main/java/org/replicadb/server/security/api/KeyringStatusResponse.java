package org.replicadb.server.security.api;

import java.util.List;
import java.util.Set;

public record KeyringStatusResponse(Set<String> knownVersions, String currentVersion,
                                    List<KeyringEnvelopeCountResponse> envelopes,
                                    long reencryptionRequired, long unknownVersionCount,
                                    boolean converged) {
}
