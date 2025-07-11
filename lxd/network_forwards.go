package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/auth"
	clusterRequest "github.com/canonical/lxd/lxd/cluster/request"
	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/lifecycle"
	"github.com/canonical/lxd/lxd/network"
	"github.com/canonical/lxd/lxd/project"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/entity"
	"github.com/canonical/lxd/shared/version"
)

var networkForwardsCmd = APIEndpoint{
	Path:        "networks/{networkName}/forwards",
	MetricsType: entity.TypeNetwork,

	Get:  APIEndpointAction{Handler: networkForwardsGet, AccessHandler: networkAccessHandler(auth.EntitlementCanView)},
	Post: APIEndpointAction{Handler: networkForwardsPost, AccessHandler: networkAccessHandler(auth.EntitlementCanEdit)},
}

var networkForwardCmd = APIEndpoint{
	Path:        "networks/{networkName}/forwards/{listenAddress}",
	MetricsType: entity.TypeNetwork,

	Delete: APIEndpointAction{Handler: networkForwardDelete, AccessHandler: networkAccessHandler(auth.EntitlementCanEdit)},
	Get:    APIEndpointAction{Handler: networkForwardGet, AccessHandler: networkAccessHandler(auth.EntitlementCanView)},
	Put:    APIEndpointAction{Handler: networkForwardPut, AccessHandler: networkAccessHandler(auth.EntitlementCanEdit)},
	Patch:  APIEndpointAction{Handler: networkForwardPut, AccessHandler: networkAccessHandler(auth.EntitlementCanEdit)},
}

// API endpoints

// swagger:operation GET /1.0/networks/{networkName}/forwards network-forwards network_forwards_get
//
//  Get the network address forwards
//
//  Returns a list of network address forwards (URLs).
//
//  ---
//  produces:
//    - application/json
//  parameters:
//    - in: query
//      name: project
//      description: Project name
//      type: string
//      example: default
//  responses:
//    "200":
//      description: API endpoints
//      schema:
//        type: object
//        description: Sync response
//        properties:
//          type:
//            type: string
//            description: Response type
//            example: sync
//          status:
//            type: string
//            description: Status description
//            example: Success
//          status_code:
//            type: integer
//            description: Status code
//            example: 200
//          metadata:
//            type: array
//            description: List of endpoints
//            items:
//              type: string
//            example: |-
//              [
//                "/1.0/networks/lxdbr0/forwards/192.0.2.1",
//                "/1.0/networks/lxdbr0/forwards/192.0.2.2"
//              ]
//    "403":
//      $ref: "#/responses/Forbidden"
//    "500":
//      $ref: "#/responses/InternalServerError"

// swagger:operation GET /1.0/networks/{networkName}/forwards?recursion=1 network-forwards network_forward_get_recursion1
//
//	Get the network address forwards
//
//	Returns a list of network address forwards (structs).
//
//	---
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	responses:
//	  "200":
//	    description: API endpoints
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          type: array
//	          description: List of network address forwards
//	          items:
//	            $ref: "#/definitions/NetworkForward"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func networkForwardsGet(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	var effectiveProjectName string
	reqInfo := request.GetContextInfo(r.Context())
	if reqInfo != nil {
		effectiveProjectName = reqInfo.EffectiveProjectName
	}

	details, err := request.GetContextValue[networkDetails](r.Context(), ctxNetworkDetails)
	if err != nil {
		return response.SmartError(err)
	}

	n, err := network.LoadByName(s, effectiveProjectName, details.networkName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network: %w", err))
	}

	// Check if project allows access to network.
	if !project.NetworkAllowed(details.requestProject.Config, details.networkName, n.IsManaged()) {
		return response.SmartError(api.StatusErrorf(http.StatusNotFound, "Network not found"))
	}

	if !n.Info().AddressForwards {
		return response.BadRequest(fmt.Errorf("Network driver %q does not support forwards", n.Type()))
	}

	memberSpecific := false // Get forwards for all cluster members.

	if util.IsRecursionRequest(r) {
		var records map[int64]*api.NetworkForward

		err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
			records, err = tx.GetNetworkForwards(ctx, n.ID(), memberSpecific)

			return err
		})
		if err != nil {
			return response.SmartError(fmt.Errorf("Failed loading network forwards: %w", err))
		}

		forwards := make([]*api.NetworkForward, 0, len(records))
		for _, record := range records {
			forwards = append(forwards, record)
		}

		return response.SyncResponse(true, forwards)
	}

	var listenAddresses map[int64]string

	err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
		listenAddresses, err = tx.GetNetworkForwardListenAddresses(ctx, n.ID(), memberSpecific)

		return err
	})
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network forwards: %w", err))
	}

	forwardURLs := make([]string, 0, len(listenAddresses))
	for _, listenAddress := range listenAddresses {
		forwardURLs = append(forwardURLs, fmt.Sprintf("/%s/networks/%s/forwards/%s", version.APIVersion, url.PathEscape(n.Name()), url.PathEscape(listenAddress)))
	}

	return response.SyncResponse(true, forwardURLs)
}

// swagger:operation POST /1.0/networks/{networkName}/forwards network-forwards network_forwards_post
//
//	Add a network address forward
//
//	Creates a new network address forward.
//
//	---
//	consumes:
//	  - application/json
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	  - in: body
//	    name: forward
//	    description: Forward
//	    required: true
//	    schema:
//	      $ref: "#/definitions/NetworkForwardsPost"
//	responses:
//	  "200":
//	    $ref: "#/responses/EmptySyncResponse"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func networkForwardsPost(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	target := request.QueryParam(r, "target")
	resp := forwardedResponseToNode(r.Context(), s, target)
	if resp != nil {
		return resp
	}

	var effectiveProjectName string
	reqInfo := request.GetContextInfo(r.Context())
	if reqInfo != nil {
		effectiveProjectName = reqInfo.EffectiveProjectName
	}

	details, err := request.GetContextValue[networkDetails](r.Context(), ctxNetworkDetails)
	if err != nil {
		return response.SmartError(err)
	}

	// Parse the request into a record.
	req := api.NetworkForwardsPost{}
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return response.BadRequest(err)
	}

	req.Normalise() // So we handle the request in normalised/canonical form.

	n, err := network.LoadByName(s, effectiveProjectName, details.networkName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network: %w", err))
	}

	// Check if project allows access to network.
	if !project.NetworkAllowed(details.requestProject.Config, details.networkName, n.IsManaged()) {
		return response.SmartError(api.StatusErrorf(http.StatusNotFound, "Network not found"))
	}

	if !n.Info().AddressForwards {
		return response.BadRequest(fmt.Errorf("Network driver %q does not support forwards", n.Type()))
	}

	clientType := clusterRequest.UserAgentClientType(r.Header.Get("User-Agent"))

	listenAddress, err := n.ForwardCreate(req, clientType)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed creating forward: %w", err))
	}

	lc := lifecycle.NetworkForwardCreated.Event(n, listenAddress.String(), request.CreateRequestor(r.Context()), nil)
	s.Events.SendLifecycle(effectiveProjectName, lc)

	return response.SyncResponseLocation(true, nil, lc.Source)
}

// swagger:operation DELETE /1.0/networks/{networkName}/forwards/{listenAddress} network-forwards network_forward_delete
//
//	Delete the network address forward
//
//	Removes the network address forward.
//
//	---
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	responses:
//	  "200":
//	    $ref: "#/responses/EmptySyncResponse"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func networkForwardDelete(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	target := request.QueryParam(r, "target")
	resp := forwardedResponseToNode(r.Context(), s, target)
	if resp != nil {
		return resp
	}

	var effectiveProjectName string
	reqInfo := request.GetContextInfo(r.Context())
	if reqInfo != nil {
		effectiveProjectName = reqInfo.EffectiveProjectName
	}

	details, err := request.GetContextValue[networkDetails](r.Context(), ctxNetworkDetails)
	if err != nil {
		return response.SmartError(err)
	}

	n, err := network.LoadByName(s, effectiveProjectName, details.networkName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network: %w", err))
	}

	// Check if project allows access to network.
	if !project.NetworkAllowed(details.requestProject.Config, details.networkName, n.IsManaged()) {
		return response.SmartError(api.StatusErrorf(http.StatusNotFound, "Network not found"))
	}

	if !n.Info().AddressForwards {
		return response.BadRequest(fmt.Errorf("Network driver %q does not support forwards", n.Type()))
	}

	listenAddress, err := url.PathUnescape(mux.Vars(r)["listenAddress"])
	if err != nil {
		return response.SmartError(err)
	}

	clientType := clusterRequest.UserAgentClientType(r.Header.Get("User-Agent"))

	err = n.ForwardDelete(listenAddress, clientType)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed deleting forward: %w", err))
	}

	s.Events.SendLifecycle(effectiveProjectName, lifecycle.NetworkForwardDeleted.Event(n, listenAddress, request.CreateRequestor(r.Context()), nil))

	return response.EmptySyncResponse
}

// swagger:operation GET /1.0/networks/{networkName}/forwards/{listenAddress} network-forwards network_forward_get
//
//	Get the network address forward
//
//	Gets a specific network address forward.
//
//	---
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	responses:
//	  "200":
//	    description: Address forward
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          $ref: "#/definitions/NetworkForward"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func networkForwardGet(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	target := request.QueryParam(r, "target")
	resp := forwardedResponseToNode(r.Context(), s, target)
	if resp != nil {
		return resp
	}

	var effectiveProjectName string
	reqInfo := request.GetContextInfo(r.Context())
	if reqInfo != nil {
		effectiveProjectName = reqInfo.EffectiveProjectName
	}

	details, err := request.GetContextValue[networkDetails](r.Context(), ctxNetworkDetails)
	if err != nil {
		return response.SmartError(err)
	}

	n, err := network.LoadByName(s, effectiveProjectName, details.networkName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network: %w", err))
	}

	// Check if project allows access to network.
	if !project.NetworkAllowed(details.requestProject.Config, details.networkName, n.IsManaged()) {
		return response.SmartError(api.StatusErrorf(http.StatusNotFound, "Network not found"))
	}

	if !n.Info().AddressForwards {
		return response.BadRequest(fmt.Errorf("Network driver %q does not support forwards", n.Type()))
	}

	listenAddress, err := url.PathUnescape(mux.Vars(r)["listenAddress"])
	if err != nil {
		return response.SmartError(err)
	}

	targetMember := request.QueryParam(r, "target")
	memberSpecific := targetMember != ""

	var forward *api.NetworkForward

	err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
		_, forward, err = tx.GetNetworkForward(ctx, n.ID(), memberSpecific, listenAddress)

		return err
	})
	if err != nil {
		return response.SmartError(err)
	}

	return response.SyncResponseETag(true, forward, forward.Etag())
}

// swagger:operation PATCH /1.0/networks/{networkName}/forwards/{listenAddress} network-forwards network_forward_patch
//
//  Partially update the network address forward
//
//  Updates a subset of the network address forward configuration.
//
//  ---
//  consumes:
//    - application/json
//  produces:
//    - application/json
//  parameters:
//    - in: query
//      name: project
//      description: Project name
//      type: string
//      example: default
//    - in: body
//      name: forward
//      description: Address forward configuration
//      required: true
//      schema:
//        $ref: "#/definitions/NetworkForwardPut"
//  responses:
//    "200":
//      $ref: "#/responses/EmptySyncResponse"
//    "400":
//      $ref: "#/responses/BadRequest"
//    "403":
//      $ref: "#/responses/Forbidden"
//    "412":
//      $ref: "#/responses/PreconditionFailed"
//    "500":
//      $ref: "#/responses/InternalServerError"

// swagger:operation PUT /1.0/networks/{networkName}/forwards/{listenAddress} network-forwards network_forward_put
//
//	Update the network address forward
//
//	Updates the entire network address forward configuration.
//
//	---
//	consumes:
//	  - application/json
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	  - in: body
//	    name: forward
//	    description: Address forward configuration
//	    required: true
//	    schema:
//	      $ref: "#/definitions/NetworkForwardPut"
//	responses:
//	  "200":
//	    $ref: "#/responses/EmptySyncResponse"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "412":
//	    $ref: "#/responses/PreconditionFailed"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func networkForwardPut(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	target := request.QueryParam(r, "target")
	resp := forwardedResponseToNode(r.Context(), s, target)
	if resp != nil {
		return resp
	}

	var effectiveProjectName string
	reqInfo := request.GetContextInfo(r.Context())
	if reqInfo != nil {
		effectiveProjectName = reqInfo.EffectiveProjectName
	}

	details, err := request.GetContextValue[networkDetails](r.Context(), ctxNetworkDetails)
	if err != nil {
		return response.SmartError(err)
	}

	n, err := network.LoadByName(s, effectiveProjectName, details.networkName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed loading network: %w", err))
	}

	// Check if project allows access to network.
	if !project.NetworkAllowed(details.requestProject.Config, details.networkName, n.IsManaged()) {
		return response.SmartError(api.StatusErrorf(http.StatusNotFound, "Network not found"))
	}

	if !n.Info().AddressForwards {
		return response.BadRequest(fmt.Errorf("Network driver %q does not support forwards", n.Type()))
	}

	listenAddress, err := url.PathUnescape(mux.Vars(r)["listenAddress"])
	if err != nil {
		return response.SmartError(err)
	}

	// Decode the request.
	req := api.NetworkForwardPut{}
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return response.BadRequest(err)
	}

	targetMember := request.QueryParam(r, "target")
	memberSpecific := targetMember != ""

	if r.Method == http.MethodPatch {
		var forward *api.NetworkForward

		err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
			_, forward, err = tx.GetNetworkForward(ctx, n.ID(), memberSpecific, listenAddress)

			return err
		})
		if err != nil {
			return response.SmartError(err)
		}

		// If config being updated via "patch" method, then merge all existing config with the keys that
		// are present in the request config.
		for k, v := range forward.Config {
			_, ok := req.Config[k]
			if !ok {
				req.Config[k] = v
			}
		}

		// If forward being updated via "patch" method and ports not specified, then merge existing ports
		// into forward.
		if req.Ports == nil {
			req.Ports = forward.Ports
		}
	}

	req.Normalise() // So we handle the request in normalised/canonical form.

	clientType := clusterRequest.UserAgentClientType(r.Header.Get("User-Agent"))

	err = n.ForwardUpdate(listenAddress, req, clientType)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed updating forward: %w", err))
	}

	s.Events.SendLifecycle(effectiveProjectName, lifecycle.NetworkForwardUpdated.Event(n, listenAddress, request.CreateRequestor(r.Context()), nil))

	return response.EmptySyncResponse
}
