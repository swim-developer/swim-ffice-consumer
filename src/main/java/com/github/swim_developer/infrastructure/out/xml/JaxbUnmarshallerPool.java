package com.github.swim_developer.infrastructure.out.xml;

import aero.fixm.ffice.FficeMessageType;
import aero.fixm.ffice.validation.FficeUnmarshallerPool;
import com.github.swim_developer.framework.application.port.out.SwimXmlUnmarshallerPort;
import com.github.swim_developer.framework.domain.exception.XmlValidationException;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class JaxbUnmarshallerPool implements SwimXmlUnmarshallerPort<FficeMessageType> {

    private final FficeUnmarshallerPool pool;

    public JaxbUnmarshallerPool() {
        this.pool = new FficeUnmarshallerPool();
        log.info("FF-ICE JAXB unmarshaller pool initialized");
    }

    @Override
    public FficeMessageType unmarshalAndValidate(String xml) throws XmlValidationException {
        try {
            Object result = pool.unmarshalAndValidate(xml);
            if (result instanceof FficeMessageType fficeMessage) {
                return fficeMessage;
            }
            throw new XmlValidationException("Unexpected JAXB root type: " + result.getClass().getName());
        } catch (FficeUnmarshallerPool.FficeUnmarshalException e) {
            throw new XmlValidationException(e.getMessage(), e);
        }
    }
}
