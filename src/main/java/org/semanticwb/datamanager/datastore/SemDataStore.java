/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.datamanager.datastore;

import org.semanticwb.store.Graph;

/**
 *
 * @author javiersolis
 */
public interface SemDataStore {
    public Graph getGraph(String modelid);
}
