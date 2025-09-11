package io.kestra.plugin.jdbc.as400;

import com.ibm.as400.access.SignonEvent;
import com.ibm.as400.access.SignonHandlerAdapter;

final class NonInteractiveSignonHandler extends SignonHandlerAdapter {
    @Override
    public boolean passwordAboutToExpire(SignonEvent event, int daysUntilExpiration) {
        return false;
    }

    @Override
    public boolean passwordExpired(SignonEvent event) {
        return false;
    }

    @Override
    public boolean connectionInitiated(SignonEvent event, boolean forceUpdate) {
        return true;
    }
}
