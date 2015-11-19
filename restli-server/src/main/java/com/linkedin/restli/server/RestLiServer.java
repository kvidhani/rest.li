/*
   Copyright (c) 2012 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.linkedin.restli.server;


import com.linkedin.common.callback.Callback;
import com.linkedin.data.ByteString;
import com.linkedin.jersey.api.uri.UriBuilder;
import com.linkedin.multipart.MultiPartMIMEReader;
import com.linkedin.multipart.MultiPartMIMEReaderCallback;
import com.linkedin.multipart.MultiPartMIMEStreamResponseFactory;
import com.linkedin.multipart.MultiPartMIMEWriter;
import com.linkedin.multipart.SinglePartMIMEReaderCallback;
import com.linkedin.multipart.exceptions.MultiPartIllegalFormatException;
import com.linkedin.parseq.Engine;
import com.linkedin.r2.message.Messages;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.stream.entitystream.ByteStringWriter;
import com.linkedin.r2.util.URIUtil;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.ProtocolVersion;
import com.linkedin.restli.common.RestConstants;
import com.linkedin.restli.common.attachments.RestLiAttachmentReader;
import com.linkedin.restli.common.attachments.RestLiStreamingAttachments;
import com.linkedin.restli.internal.common.AllProtocolVersions;
import com.linkedin.restli.internal.common.ProtocolVersionUtil;
import com.linkedin.restli.internal.common.attachments.AttachmentUtilities;
import com.linkedin.restli.internal.server.RestLiCallback;
import com.linkedin.restli.internal.server.RestLiMethodInvoker;
import com.linkedin.restli.internal.server.RestLiResponseHandler;
import com.linkedin.restli.internal.server.RestLiRouter;
import com.linkedin.restli.internal.server.RoutingResult;
import com.linkedin.restli.internal.server.ServerResourceContext;
import com.linkedin.restli.internal.server.filter.FilterRequestContextInternal;
import com.linkedin.restli.internal.server.filter.FilterRequestContextInternalImpl;
import com.linkedin.restli.internal.server.methods.response.ErrorResponseBuilder;
import com.linkedin.restli.internal.server.model.ResourceMethodDescriptor;
import com.linkedin.restli.internal.server.model.ResourceMethodDescriptor.InterfaceType;
import com.linkedin.restli.internal.server.model.ResourceModel;
import com.linkedin.restli.internal.server.model.RestLiApiBuilder;
import com.linkedin.restli.internal.server.util.MIMEParse;
import com.linkedin.restli.server.filter.ResponseFilter;
import com.linkedin.restli.server.multiplexer.MultiplexedRequestHandler;
import com.linkedin.restli.server.multiplexer.MultiplexedRequestHandlerImpl;
import com.linkedin.restli.server.resources.PrototypeResourceFactory;
import com.linkedin.restli.server.resources.ResourceFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author dellamag
 * @author Zhenkai Zhu
 * @author nshankar
 * @author Karim Vidhani
 */
//TODO: Remove this once use of InvokeAware has been discontinued.
@SuppressWarnings("deprecation")
public class RestLiServer extends BaseRestServer
{
  public static final String DEBUG_PATH_SEGMENT = "__debug";

  private static final Logger log = LoggerFactory.getLogger(RestLiServer.class);

  private final RestLiConfig _config;
  private final RestLiRouter _router;
  private final ResourceFactory _resourceFactory;
  private final RestLiMethodInvoker _methodInvoker;
  private final RestLiResponseHandler _responseHandler;
  private final RestLiDocumentationRequestHandler _docRequestHandler;
  private final MultiplexedRequestHandler _multiplexedRequestHandler;
  private final ErrorResponseBuilder _errorResponseBuilder;
  private final Map<String, RestLiDebugRequestHandler> _debugHandlers;
  private final List<ResponseFilter> _responseFilters;
  private final List<InvokeAware> _invokeAwares;
  private boolean _isDocInitialized = false;

  public RestLiServer(final RestLiConfig config)
  {
    this(config, new PrototypeResourceFactory());
  }

  public RestLiServer(final RestLiConfig config, final ResourceFactory resourceFactory)
  {
    this(config, resourceFactory, null);
  }

  public RestLiServer(final RestLiConfig config, final ResourceFactory resourceFactory, final Engine engine)
  {
    this(config, resourceFactory, engine, null);
  }

  @Deprecated
  public RestLiServer(final RestLiConfig config,
                      final ResourceFactory resourceFactory,
                      final Engine engine,
                      final List<InvokeAware> invokeAwares)
  {
    super(config);
    _config = config;
    _errorResponseBuilder = new ErrorResponseBuilder(config.getErrorResponseFormat(), config.getInternalErrorMessage());
    _resourceFactory = resourceFactory;
    _rootResources = new RestLiApiBuilder(config).build();
    _resourceFactory.setRootResources(_rootResources);
    _router = new RestLiRouter(_rootResources);
    _methodInvoker =
        new RestLiMethodInvoker(_resourceFactory, engine, _errorResponseBuilder, config.getRequestFilters());
    _responseHandler =
        new RestLiResponseHandler.Builder().setErrorResponseBuilder(_errorResponseBuilder)
                                           .build();
    _docRequestHandler = config.getDocumentationRequestHandler();
    _debugHandlers = new HashMap<String, RestLiDebugRequestHandler>();
    if (config.getResponseFilters() != null)
    {
      _responseFilters = config.getResponseFilters();
    }
    else
    {
      _responseFilters = new ArrayList<ResponseFilter>();
    }
    for (RestLiDebugRequestHandler debugHandler : config.getDebugRequestHandlers())
    {
      _debugHandlers.put(debugHandler.getHandlerId(), debugHandler);
    }

    _multiplexedRequestHandler = new MultiplexedRequestHandlerImpl(this,
                                                                   engine,
                                                                   config.getMaxRequestsMultiplexed(),
                                                                   config.getMultiplexerSingletonFilter());

    // verify that if there are resources using the engine, then the engine is not null
    if (engine == null)
    {
      for (ResourceModel model : _rootResources.values())
      {
        for (ResourceMethodDescriptor desc : model.getResourceMethodDescriptors())
        {
          final InterfaceType type = desc.getInterfaceType();
          if (type == InterfaceType.PROMISE || type == InterfaceType.TASK)
          {
            final String fmt =
                "ParSeq based method %s.%s, but no engine given. "
                    + "Check your RestLiServer construction, spring wiring, "
                    + "and container-pegasus-restli-server-cmpt version.";
            log.warn(String.format(fmt, model.getResourceClass().getName(), desc.getMethod().getName()));
          }
        }
      }
    }
    _invokeAwares =
        (invokeAwares == null) ? Collections.<InvokeAware> emptyList() : Collections.unmodifiableList(invokeAwares);
  }

  public Map<String, ResourceModel> getRootResources()
  {
    return Collections.unmodifiableMap(_rootResources);
  }

  /**
   * @see BaseRestServer#doHandleRequest(com.linkedin.r2.message.rest.RestRequest,
   *      com.linkedin.r2.message.RequestContext, com.linkedin.common.callback.Callback)
   */
  @Override
  protected void doHandleRequest(final RestRequest request,
                                 final RequestContext requestContext,
                                 final Callback<RestResponse> callback)
  {
    //Until RestRequest is removed, this code path cannot accept content types or accept types that contain
    //multipart/related. This is because these types of requests will usually have very large payloads and therefore
    //would degrade server performance since RestRequest reads everything into memory.
    verifyAttachmentSupportNotNeeded(request, callback);

    if (isDocumentationRequest(request))
    {
      handleDocumentationRequest(request, callback);
    }
    else if (isMultiplexedRequest(request))
    {
      handleMultiplexedRequest(request, requestContext, callback);
    }
    else
    {
      RestLiDebugRequestHandler debugHandlerForRequest = findDebugRequestHandler(request);

      if (debugHandlerForRequest != null)
      {
        handleDebugRequest(debugHandlerForRequest, request, requestContext, callback);
      }
      else
      {
        handleResourceRequest(request,
                              requestContext,
                              new RequestExecutionCallbackAdapter<RestResponse>(callback),
                              null,
                              false);
      }
    }
  }

  private boolean isSupportedProtocolVersion(ProtocolVersion clientProtocolVersion,
                                             ProtocolVersion lowerBound,
                                             ProtocolVersion upperBound)
  {
    int lowerCheck = clientProtocolVersion.compareTo(lowerBound);
    int upperCheck = clientProtocolVersion.compareTo(upperBound);
    return lowerCheck >= 0 && upperCheck <= 0;
  }

  /**
   * Ensures that the Rest.li protocol version used by the client is valid
   *
   * (assume the protocol version used by the client is "v")
   *
   * v is valid if {@link com.linkedin.restli.internal.common.AllProtocolVersions#OLDEST_SUPPORTED_PROTOCOL_VERSION}
   * <= v <= {@link com.linkedin.restli.internal.common.AllProtocolVersions#NEXT_PROTOCOL_VERSION}
   *
   * @param request
   *          the incoming request from the client
   * @throws RestLiServiceException
   *           if the protocol version used by the client is not valid based on the rules described
   *           above
   */
  private void ensureRequestUsesValidRestliProtocol(final RestRequest request) throws RestLiServiceException
  {
    ProtocolVersion clientProtocolVersion = ProtocolVersionUtil.extractProtocolVersion(request.getHeaders());
    ProtocolVersion lowerBound = AllProtocolVersions.OLDEST_SUPPORTED_PROTOCOL_VERSION;
    ProtocolVersion upperBound = AllProtocolVersions.NEXT_PROTOCOL_VERSION;
    if (!isSupportedProtocolVersion(clientProtocolVersion, lowerBound, upperBound))
    {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Rest.li protocol version "
          + clientProtocolVersion + " used by the client is not supported!");
    }
  }

  private void handleDebugRequest(final RestLiDebugRequestHandler debugHandler,
                                  final RestRequest request,
                                  final RequestContext requestContext,
                                  final Callback<RestResponse> callback)
  {
    debugHandler.handleRequest(request, requestContext, new RestLiDebugRequestHandler.ResourceDebugRequestHandler()
    {
      @Override
      public void handleRequest(final RestRequest request,
                                final RequestContext requestContext,
                                final RequestExecutionCallback<RestResponse> callback)
      {
        // Create a new request at this point from the debug request by removing the path suffix
        // starting with "__debug".
        String fullPath = request.getURI().getPath();
        int debugSegmentIndex = fullPath.indexOf(DEBUG_PATH_SEGMENT);

        RestRequestBuilder requestBuilder = new RestRequestBuilder(request);

        UriBuilder uriBuilder = UriBuilder.fromUri(request.getURI());
        uriBuilder.replacePath(request.getURI().getPath().substring(0, debugSegmentIndex - 1));
        requestBuilder.setURI(uriBuilder.build());

        handleResourceRequest(requestBuilder.build(), requestContext, callback, null, true);
      }
    }, callback);
  }

  private void handleResourceRequest(final RestRequest request,
                                     final RequestContext requestContext,
                                     final RequestExecutionCallback<RestResponse> callback,
                                     final RestLiAttachmentReader attachmentReader,
                                     final boolean isDebugMode)
  {
    try
    {
      ensureRequestUsesValidRestliProtocol(request);
    }
    catch (RestLiServiceException e)
    {
      final RestLiCallback<Object> restLiCallback =
          new RestLiCallback<Object>(request, null, _responseHandler, callback, null, null);
      restLiCallback.onError(e, createEmptyExecutionReport());
      return;
    }
    final RoutingResult method;
    try
    {
      method = _router.process(request, requestContext, attachmentReader);
    }
    catch (Exception e)
    {
      final RestLiCallback<Object> restLiCallback =
          new RestLiCallback<Object>(request, null, _responseHandler, callback, null, null);
      restLiCallback.onError(e, createEmptyExecutionReport());
      return;
    }
    final RequestExecutionCallback<RestResponse> wrappedCallback = notifyInvokeAwares(method, callback);

    final FilterRequestContextInternal filterContext =
        new FilterRequestContextInternalImpl((ServerResourceContext) method.getContext(), method.getResourceMethod());
    final RestLiCallback<Object> restLiCallback =
        new RestLiCallback<Object>(request, method, _responseHandler, wrappedCallback, _responseFilters, filterContext);
    try
    {
      _methodInvoker.invoke(method, request, restLiCallback, isDebugMode, filterContext);
    }
    catch (Exception e)
    {
      restLiCallback.onError(e, createEmptyExecutionReport());
    }
  }

  /**
   * Invoke {@link InvokeAware#onInvoke(ResourceContext, RestLiMethodContext)} of registered invokeAwares.
   * @return A new callback that wraps the originalCallback, which invokes desired callbacks of invokeAwares after the method invocation finishes
   */
  private RequestExecutionCallback<RestResponse> notifyInvokeAwares(final RoutingResult routingResult,
                                                                    final RequestExecutionCallback<RestResponse> originalCallback)
  {
    if (!_invokeAwares.isEmpty())
    {
      final List<Callback<RestResponse>> invokeAwareCallbacks = new ArrayList<Callback<RestResponse>>();
      for (InvokeAware invokeAware : _invokeAwares)
      {
        invokeAwareCallbacks.add(invokeAware.onInvoke(routingResult.getContext(), routingResult.getResourceMethod()));
      }

      return new RequestExecutionCallback<RestResponse>()
      {
        @Override
        public void onSuccess(RestResponse result, RequestExecutionReport executionReport, RestLiStreamingAttachments attachments)
        {
          for (Callback<RestResponse> callback : invokeAwareCallbacks)
          {
            callback.onSuccess(result);
          }
          originalCallback.onSuccess(result, executionReport, attachments);
        }

        @Override
        public void onError(Throwable error, RequestExecutionReport executionReport)
        {
          for (Callback<RestResponse> callback : invokeAwareCallbacks)
          {
            callback.onError(error);
          }
          originalCallback.onError(error, executionReport);
        }
      };
    }

    return originalCallback;
  }


  private boolean isMultiplexedRequest(Request request)
  {
    return _multiplexedRequestHandler.isMultiplexedRequest(request);
  }

  private void handleMultiplexedRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback)
  {
    _multiplexedRequestHandler.handleRequest(request, requestContext, callback);
  }

  private boolean isDocumentationRequest(Request request)
  {
    return _docRequestHandler != null && _docRequestHandler.isDocumentationRequest(request);
  }

  private void handleDocumentationRequest(final RestRequest request, final Callback<RestResponse> callback)
  {
    try
    {
      synchronized (this)
      {
        if (!_isDocInitialized)
        {
          _docRequestHandler.initialize(_config, _rootResources);
          _isDocInitialized = true;
        }
      }

      final RestResponse response = _docRequestHandler.processDocumentationRequest(request);
      callback.onSuccess(response);
    }
    catch (Exception e)
    {
      final RestLiCallback<Object> restLiCallback =
          new RestLiCallback<Object>(request,
                                     null,
                                     _responseHandler,
                                     new RequestExecutionCallbackAdapter<RestResponse>(callback),
                                     null,
                                     null);
      restLiCallback.onError(e, createEmptyExecutionReport());
    }
  }

  private RestLiDebugRequestHandler findDebugRequestHandler(Request request)
  {
    String[] pathSegments = URIUtil.tokenizePath(request.getURI().getPath());
    String debugHandlerId = null;
    RestLiDebugRequestHandler resultDebugHandler = null;

    for (int i = 0; i < pathSegments.length; ++i)
    {
      String pathSegment = pathSegments[i];
      if (pathSegment.equals(DEBUG_PATH_SEGMENT))
      {
        if (i < pathSegments.length - 1)
        {
          debugHandlerId = pathSegments[i + 1];
        }

        break;
      }
    }

    if (debugHandlerId != null)
    {
      resultDebugHandler = _debugHandlers.get(debugHandlerId);
    }

    return resultDebugHandler;
  }

  private static RequestExecutionReport createEmptyExecutionReport()
  {
    return new RequestExecutionReportBuilder().build();
  }

  private class RequestExecutionCallbackAdapter<T> implements RequestExecutionCallback<T>
  {
    private final Callback<T> _wrappedCallback;

    public RequestExecutionCallbackAdapter(Callback<T> wrappedCallback)
    {
      _wrappedCallback = wrappedCallback;
    }

    @Override
    public void onError(Throwable e, RequestExecutionReport executionReport)
    {
      _wrappedCallback.onError(e);
    }

    @Override
    public void onSuccess(T result, RequestExecutionReport executionReport, RestLiStreamingAttachments attachments)
    {
      _wrappedCallback.onSuccess(result);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Streaming related functionality defined here.
  //In the future we will deprecate and remove RestRequest/RestResponse. Until then we need to be minimally invasive
  //while still offering existing functionality.

  /**
   * @see BaseRestServer#doHandleStreamRequest(com.linkedin.r2.message.rest.StreamRequest,
   *      com.linkedin.r2.message.RequestContext, com.linkedin.common.callback.Callback)
   */
  @Override
  protected void doHandleStreamRequest(final StreamRequest request,
                                       final RequestContext requestContext,
                                       final Callback<StreamResponse> callback)
  {
    //Eventually - when RestRequest is removed, we will migrate all of these code paths to StreamRequest.

    //For documentation requests, it is important to note that the payload is ignored therefore we can just read
    //everything into memory.
    if (isDocumentationRequest(request))
    {
      Messages.toRestRequest(request, new Callback<RestRequest>()
      {
        @Override
        public void onError(Throwable e)
        {
          callback.onError(e);
        }

        @Override
        public void onSuccess(RestRequest result)
        {
          handleDocumentationRequest(result, Messages.toRestCallback(callback));
        }
      });
    }
    //For multiplexed requests, we read everything into memory. If individual requests specify multipart/related
    //as a content type or accept type, a bad request will be thrown later when the multiplexer calls
    //handleRequest(RestRequest request ....) for each individual request.
    else if (isMultiplexedRequest(request))
    {
      Messages.toRestRequest(request, new Callback<RestRequest>()
      {
        @Override
        public void onError(Throwable e)
        {
          callback.onError(e);
        }

        @Override
        public void onSuccess(RestRequest result)
        {
          handleMultiplexedRequest(result, requestContext, Messages.toRestCallback(callback));
        }
      });
    }
    else
    {
      final RestLiDebugRequestHandler debugHandlerForRequest = findDebugRequestHandler(request);

      if (debugHandlerForRequest != null)
      {
        //Currently we will not support debugging + attachment support.
        verifyAttachmentSupportNotNeeded(request, callback);

        Messages.toRestRequest(request, new Callback<RestRequest>()
        {
          @Override
          public void onError(Throwable e)
          {
            callback.onError(e);
          }

          @Override
          public void onSuccess(RestRequest result)
          {
            handleDebugRequest(debugHandlerForRequest, result, requestContext, Messages.toRestCallback(callback));
          }
        });
      }
      else
      {
        //At this point we need to check the content-type to understand how we should handle the request.
        String header = request.getHeader(RestConstants.HEADER_CONTENT_TYPE);
        if (header != null)
        {
          ContentType contentType = null;
          try
          {
            contentType = new ContentType(header);
          }
          catch (ParseException e)
          {
            callback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Unable to parse Content-Type: " + header));
          }

          if (contentType.getBaseType().equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
          {
            //We need to reconstruct a RestRequest that has the first part of the multipart/related payload as the
            //traditional metadata payload of a RestRequest.
            final MultiPartMIMEReader multiPartMIMEReader = MultiPartMIMEReader.createAndAcquireStream(request);
            final RestRequestBuilder restRequestBuilder = new RestRequestBuilder(request);
            final TopLevelReaderCallback firstPartReader =
                new TopLevelReaderCallback(restRequestBuilder, requestContext, callback, multiPartMIMEReader);
            multiPartMIMEReader.registerReaderCallback(firstPartReader);
            return;
          }
        }

        //If we get here this means that the content-type is missing (which is supported to maintain backwards compatibility)
        //or that it exists and is something other then multipart/related. This means we can read the entire payload into memory
        //and reconstruct the RestRequest.
        Messages.toRestRequest(request, new Callback<RestRequest>()
        {
          @Override
          public void onError(Throwable e)
          {
            callback.onError(e);
          }

          @Override
          public void onSuccess(RestRequest result)
          {
            //This callback is invoked once the incoming StreamRequest is converted into a RestRequest. We can now
            //move forward with this request.
            //It is important to note that the server's response may include attachments so we factor that into
            //consideration upon completion of this request.
            final StreamingCallbackAdaptor streamingCallbackAdaptor = new StreamingCallbackAdaptor(callback);
            handleResourceRequest(result, requestContext, streamingCallbackAdaptor, null, false);
          }
        });
      }
    }
  }

  private class TopLevelReaderCallback implements MultiPartMIMEReaderCallback
  {
    private final RestRequestBuilder _restRequestBuilder;
    private volatile ByteString _requestPayload = null;
    private final RequestContext _requestContext;
    private final Callback<StreamResponse> _streamResponseCallback;
    private final MultiPartMIMEReader _multiPartMIMEReader;

    private TopLevelReaderCallback(final RestRequestBuilder restRequestBuilder, final RequestContext requestContext,
                                   final Callback<StreamResponse> streamResponseCallback, final MultiPartMIMEReader multiPartMIMEReader)
    {
      _restRequestBuilder = restRequestBuilder;
      _requestContext = requestContext;
      _streamResponseCallback = streamResponseCallback;
      _multiPartMIMEReader = multiPartMIMEReader;
    }

    private void setRequestPayload(final ByteString requestPayload)
    {
      _requestPayload = requestPayload;
    }

    @Override
    public void onNewPart(MultiPartMIMEReader.SinglePartMIMEReader singleParMIMEReader)
    {
      if (_requestPayload == null)
      {
        //The first time this is invoked we read in the first part.
        //At this point in time the Content-Type is still multipart/related for the artificially created RestRequest.
        //Therefore care must be taken to make sure that we propagate the Content-Type from the first part as the Content-Type
        //of the artificially created RestRequest.
        final Map<String, String> singlePartHeaders = singleParMIMEReader.dataSourceHeaders(); //Case-insensitive map already.
        final String contentTypeString = singlePartHeaders.get(RestConstants.HEADER_CONTENT_TYPE);
        if (contentTypeString == null)
        {
          _streamResponseCallback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
                                                                     "Incorrect multipart/related payload. First part must contain the Content-Type!"));
        }

        ContentType contentType = null;
        try
        {
          contentType = new ContentType(contentTypeString);
        }
        catch (ParseException e)
        {
          _streamResponseCallback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
                                                                     "Unable to parse Content-Type: " + contentTypeString));
        }

        final String baseType = contentType.getBaseType();
        if (!(baseType.equalsIgnoreCase(RestConstants.HEADER_VALUE_APPLICATION_JSON) ||
            baseType.equalsIgnoreCase(RestConstants.HEADER_VALUE_APPLICATION_PSON)))
        {
          _streamResponseCallback.onError(new RestLiServiceException(HttpStatus.S_415_UNSUPPORTED_MEDIA_TYPE,
                                                                     "Unknown Content-Type for first part of multipart/related payload: " + contentType.toString()));
        }

        _restRequestBuilder.setHeader(RestConstants.HEADER_CONTENT_TYPE, contentTypeString);
        FirstPartReaderCallback firstPartReaderCallback = new FirstPartReaderCallback(this, singleParMIMEReader);
        singleParMIMEReader.registerReaderCallback(firstPartReaderCallback);
        singleParMIMEReader.requestPartData();
      }
      else
      {
        //This is the beginning of the 2nd part, so pass this to the client.
        _restRequestBuilder.setEntity(_requestPayload);
        final StreamingCallbackAdaptor streamingCallbackAdaptor = new StreamingCallbackAdaptor(_streamResponseCallback);
        handleResourceRequest(_restRequestBuilder.build(), _requestContext, streamingCallbackAdaptor,
                              new RestLiAttachmentReader(_multiPartMIMEReader), false);
      }
    }

    @Override
    public void onFinished()
    {
      //Verify we actually had some parts.
      if (_requestPayload == null)
      {
        _streamResponseCallback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
                                                                   "Did not receive any parts in the multipart mime request!"));
      }

      //At this point, this means that the multipart mime envelope didn't have any attachments (apart from the
      //json/pson payload).
      //In this case we set the attachment reader to null.
      final StreamingCallbackAdaptor streamingCallbackAdaptor = new StreamingCallbackAdaptor(_streamResponseCallback);
      handleResourceRequest(_restRequestBuilder.build(), _requestContext, streamingCallbackAdaptor, null, false);
    }

    @Override
    public void onAbandoned()
    {
      _streamResponseCallback.onError(new IllegalStateException("Serious error. There should never be a call to abandon"
                                                                    + " the entire payload when decoding a multipart mime response."));
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      //At this point this could be a an exception thrown due to malformed data or this could be an exception thrown
      //due to an invocation of a callback.
      if (throwable instanceof MultiPartIllegalFormatException)
      {
        //If its an illegally formed request, then we send back 400.
        _streamResponseCallback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
                                                                   "Illegally formed multipart payload", throwable));
      }
      //Otherwise this is an internal server error. R2 will convert this to a 500 for us.
      _streamResponseCallback.onError(throwable);
    }
  }

  private class FirstPartReaderCallback implements SinglePartMIMEReaderCallback
  {
    private final TopLevelReaderCallback _topLevelReaderCallback;
    private final MultiPartMIMEReader.SinglePartMIMEReader _singlePartMIMEReader;
    private final ByteString.Builder _builder = new ByteString.Builder();

    public FirstPartReaderCallback(
        final TopLevelReaderCallback topLevelReaderCallback,
        final MultiPartMIMEReader.SinglePartMIMEReader singlePartMIMEReader)
    {
      _topLevelReaderCallback = topLevelReaderCallback;
      _singlePartMIMEReader = singlePartMIMEReader;
    }

    @Override
    public void onPartDataAvailable(ByteString partData)
    {
      _builder.append(partData);
      _singlePartMIMEReader.requestPartData();
    }

    @Override
    public void onFinished()
    {
      _topLevelReaderCallback.setRequestPayload(_builder.build());
    }

    @Override
    public void onAbandoned()
    {
      _topLevelReaderCallback.onStreamError(new IllegalStateException("Serious error. There should never be a call to "
                                                                          + "abandon part data when decoding the first part in a multipart mime response."));
    }

    @Override
    public void onStreamError(Throwable throwable)
    {
      //No need to do anything as the MultiPartMIMEReader will also call onStreamError() on the top level callback
      //which will then call the response callback.
    }
  }

  private class StreamingCallbackAdaptor implements RequestExecutionCallback<RestResponse>
  {
    private final Callback<StreamResponse> _streamResponseCallback;

    private StreamingCallbackAdaptor(final Callback<StreamResponse> streamResponseCallback)
    {
      _streamResponseCallback = streamResponseCallback;
    }

    @Override
    public void onError(final Throwable e, final RequestExecutionReport executionReport)
    {
      _streamResponseCallback.onError(e);
    }

    @Override
    public void onSuccess(final RestResponse result, final RequestExecutionReport executionReport,
                          final RestLiStreamingAttachments attachments)
    {
      //Construct the StreamResponse and invoke the callback. The RestResponse entity should be the first part.
      //There may potentially be attachments included in the response .
      if (attachments != null && attachments.getStreamingDataSources().size() > 0)
      {
        final ByteStringWriter firstPartWriter = new ByteStringWriter(result.getEntity());
        final MultiPartMIMEWriter multiPartMIMEWriter = AttachmentUtilities
            .createMultiPartMIMEWriter(firstPartWriter, result.getHeader(RestConstants.HEADER_CONTENT_TYPE),
                                       attachments);

        final StreamResponse streamResponse =
            MultiPartMIMEStreamResponseFactory
                .generateMultiPartMIMEStreamResponse(AttachmentUtilities.RESTLI_MULTIPART_SUBTYPE, multiPartMIMEWriter);
        _streamResponseCallback.onSuccess(streamResponse);
      }
      else
      {
        _streamResponseCallback.onSuccess(Messages.toStreamResponse(result));
      }
    }
  }

  //Note that using the raw type here on Callback is acceptable
  private static void verifyAttachmentSupportNotNeeded(final Request request, final Callback callback)
  {
    final Map<String, String> requestHeaders = request.getHeaders();
    try
    {
      final String contentTypeString = requestHeaders.get(RestConstants.HEADER_CONTENT_TYPE);
      if (contentTypeString != null)
      {
        final ContentType contentType = new ContentType(contentTypeString);
        if (contentType.getBaseType().equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
        {
          callback.onError(new RestLiServiceException(HttpStatus.S_415_UNSUPPORTED_MEDIA_TYPE,
                                                      "This server cannot handle requests with a content type of multipart/related"));
        }
      }
      final String acceptTypeHeader = requestHeaders.get(RestConstants.HEADER_ACCEPT);
      if (acceptTypeHeader != null)
      {
        final List<String> acceptTypes = MIMEParse.parseAcceptType(acceptTypeHeader);
        for (final String acceptType : acceptTypes)
        {
          if (acceptType.equalsIgnoreCase(RestConstants.HEADER_VALUE_MULTIPART_RELATED))
          {
            callback.onError(new RestLiServiceException(HttpStatus.S_406_NOT_ACCEPTABLE,
                                                        "This server cannot handle requests with an accept type of multipart/related"));
          }
        }
      }
    }
    catch (ParseException parseException)
    {
      callback.onError(new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST));
    }
  }
}
